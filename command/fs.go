package command

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/hashicorp/nomad/api"
)

type FSCommand struct {
	Meta
}

func (f *FSCommand) Help() string {
	helpText := `
Usage: nomad fs <alloc-id> <path>

  fs displays either the contents of an allocation directory for the passed allocation,
  or displays the file at the given path. The path is relative to the root of the alloc
  dir and defaults to root if unspecified.

General Options:

  ` + generalOptionsUsage() + `

FS Specific Options:

  -H
    Machine friendly output.

  -verbose
    Show full information.

  -job <job-id>
    Use a random allocation from a specified job-id.

  -stat
    Show file stat information instead of displaying the file, or listing the directory.

`
	return strings.TrimSpace(helpText)
}

func (f *FSCommand) Synopsis() string {
	return "Inspect the contents of an allocation directory"
}

func (f *FSCommand) Run(args []string) int {
	var verbose, machine, job, stat bool
	flags := f.Meta.FlagSet("fs-list", FlagSetClient)
	flags.Usage = func() { f.Ui.Output(f.Help()) }
	flags.BoolVar(&verbose, "verbose", false, "")
	flags.BoolVar(&machine, "H", false, "")
	flags.BoolVar(&job, "job", false, "")
	flags.BoolVar(&stat, "stat", false, "")

	if err := flags.Parse(args); err != nil {
		return 1
	}
	args = flags.Args()

	if len(args) < 1 {
		if job {
			f.Ui.Error("job ID is required")
		} else {
			f.Ui.Error("allocation ID is required")
		}

		return 1
	}

	path := "/"
	if len(args) == 2 {
		path = args[1]
	}

	client, err := f.Meta.Client()
	if err != nil {
		f.Ui.Error(fmt.Sprintf("Error initializing client: %v", err))
		return 1
	}

	// If -job is specified, use random allocation, otherwise use provided allocation
	allocID := args[0]
	if job {
		allocID, err = getRandomJobAlloc(client, args[0])
		if err != nil {
			f.Ui.Error(fmt.Sprintf("Error fetching allocations: %v", err))
			return 1
		}
	}

	// Truncate the id unless full length is requested
	length := shortId
	if verbose {
		length = fullId
	}
	// Query the allocation info
	if len(allocID) == 1 {
		f.Ui.Error(fmt.Sprintf("Alloc ID must contain at least two characters."))
		return 1
	}
	if len(allocID)%2 == 1 {
		// Identifiers must be of even length, so we strip off the last byte
		// to provide a consistent user experience.
		allocID = allocID[:len(allocID)-1]
	}

	allocs, _, err := client.Allocations().PrefixList(allocID)
	if err != nil {
		f.Ui.Error(fmt.Sprintf("Error querying allocation: %v", err))
		return 1
	}
	if len(allocs) == 0 {
		f.Ui.Error(fmt.Sprintf("No allocation(s) with prefix or id %q found", allocID))
		return 1
	}
	if len(allocs) > 1 {
		// Format the allocs
		out := make([]string, len(allocs)+1)
		out[0] = "ID|Eval ID|Job ID|Task Group|Desired Status|Client Status"
		for i, alloc := range allocs {
			out[i+1] = fmt.Sprintf("%s|%s|%s|%s|%s|%s",
				limit(alloc.ID, length),
				limit(alloc.EvalID, length),
				alloc.JobID,
				alloc.TaskGroup,
				alloc.DesiredStatus,
				alloc.ClientStatus,
			)
		}
		f.Ui.Output(fmt.Sprintf("Prefix matched multiple allocations\n\n%s", formatList(out)))
		return 0
	}
	// Prefix lookup matched a single allocation
	alloc, _, err := client.Allocations().Info(allocs[0].ID, nil)
	if err != nil {
		f.Ui.Error(fmt.Sprintf("Error querying allocation: %s", err))
		return 1
	}

	if alloc.DesiredStatus == "failed" {
		allocID := limit(alloc.ID, length)
		msg := fmt.Sprintf(`The allocation %q failed to be placed. To see the cause, run:
nomad alloc-status %s`, allocID, allocID)
		f.Ui.Error(msg)
		return 0
	}

	// Get file stat info
	file, _, err := client.AllocFS().Stat(alloc, path, nil)
	if err != nil {
		f.Ui.Error(err.Error())
		return 1
	}

	// If we want file stats, print those and exit.
	if stat {
		// Display the file information
		out := make([]string, 2)
		out[0] = "Mode|Size|Modified Time|Name"
		if file != nil {
			fn := file.Name
			if file.IsDir {
				fn = fmt.Sprintf("%s/", fn)
			}
			var size string
			if machine {
				size = fmt.Sprintf("%d", file.Size)
			} else {
				size = humanize.IBytes(uint64(file.Size))
			}
			out[1] = fmt.Sprintf("%s|%s|%s|%s", file.FileMode, size,
				formatTime(file.ModTime), fn)
		}
		f.Ui.Output(formatList(out))
		return 0
	}

	// Determine if the path is a file or a directory.
	if file.IsDir {
		// We have a directory, list it.
		files, _, err := client.AllocFS().List(alloc, path, nil)
		if err != nil {
			f.Ui.Error(fmt.Sprintf("Error listing alloc dir: %s", err))
			return 1
		}
		// Display the file information in a tabular format
		out := make([]string, len(files)+1)
		out[0] = "Mode|Size|Modified Time|Name"
		for i, file := range files {
			fn := file.Name
			if file.IsDir {
				fn = fmt.Sprintf("%s/", fn)
			}
			var size string
			if machine {
				size = fmt.Sprintf("%d", file.Size)
			} else {
				size = humanize.IBytes(uint64(file.Size))
			}
			out[i+1] = fmt.Sprintf("%s|%s|%s|%s",
				file.FileMode,
				size,
				formatTime(file.ModTime),
				fn,
			)
		}
		f.Ui.Output(formatList(out))
	} else {
		// We have a file, cat it.
		r, _, err := client.AllocFS().Cat(alloc, path, nil)
		if err != nil {
			f.Ui.Error(fmt.Sprintf("Error reading file: %s", err))
			return 1
		}
		io.Copy(os.Stdout, r)
	}

	return 0
}

// Get Random Allocation ID from a known jobID. Prefer to use a running allocation,
// but use a dead allocation if no running allocations are found
func getRandomJobAlloc(client *api.Client, jobID string) (string, error) {
	var runningAllocs []*api.AllocationListStub
	allocs, _, err := client.Jobs().Allocations(jobID, nil)

	// Check that the job actually has allocations
	if len(allocs) == 0 {
		return "", fmt.Errorf("job %q doesn't exist or it has no allocations", jobID)
	}

	for _, v := range allocs {
		if v.ClientStatus == "running" {
			runningAllocs = append(runningAllocs, v)
		}
	}
	// If we don't have any allocations running, use dead allocations
	if len(runningAllocs) < 1 {
		runningAllocs = allocs
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	allocID := runningAllocs[r.Intn(len(runningAllocs))].ID
	return allocID, err
}
