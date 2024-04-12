package timer

import (
	"golang.org/x/sys/windows"
	"syscall"
	"unsafe"
)

type PageFaultHandler struct {
	pageFaultHandle      syscall.Handle
	getProcessMemoryInfo *syscall.Proc
}

func (p PageFaultHandler) GetPageFaults() int {
	var process processMemoryCounters
	process.cb = uint32(unsafe.Sizeof((process)))

	_, _, err := p.getProcessMemoryInfo.Call(uintptr(p.pageFaultHandle), uintptr(unsafe.Pointer(&process)), uintptr(process.cb))
	if err != nil {
		// TODO handle windows err
		//panic(err)
	}

	return int(process.PageFaultCount)
}

func InitPageFaultHandler() (PageFaultHandler, error) {
	handle, err := syscall.OpenProcess(windows.PROCESS_QUERY_INFORMATION|windows.PROCESS_VM_READ, false, windows.GetCurrentProcessId())
	if err != nil {
		return PageFaultHandler{}, err
	}

	psapi, err := syscall.LoadDLL("psapi.dll")
	if err != nil {
		return PageFaultHandler{}, err
	}

	getProcessMemoryInfo, err := psapi.FindProc("GetProcessMemoryInfo")
	if err != nil {
		return PageFaultHandler{}, err
	}

	return PageFaultHandler{
		pageFaultHandle:      handle,
		getProcessMemoryInfo: getProcessMemoryInfo,
	}, nil
}

type processMemoryCounters struct {
	cb                         uint32
	PageFaultCount             uint32
	PeakWorkingSetSize         uint64
	WorkingSetSize             uint64
	QuotaPeakPagedPoolUsage    uint64
	QuotaPagedPoolUsage        uint64
	QuotaPeakNonPagedPoolUsage uint64
	QuotaNonPagedPoolUsage     uint64
	PagefileUsage              uint64
	PeakPagefileUsage          uint64
}
