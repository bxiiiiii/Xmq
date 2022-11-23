package collect

import (
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/mem"
	"github.com/shirou/gopsutil/net"
)

type BrokerUsage struct {
	cpu           ResourceUsage
	virtualMemory ResourceUsage
	swapMemory    ResourceUsage
	bandwidthIn   ResourceUsage
	bandwidthOut  ResourceUsage
}

type ResourceUsage struct {
	usage float64
	limit float64
}

func CollectLoadData() (usage BrokerUsage, err error) {
	usage.cpu.usage, err = getCpu()
	if err != nil {
		return usage, err
	}

	usage.virtualMemory.usage, err = getVirtualMemory()
	if err != nil {
		return usage, err
	}

	usage.swapMemory.usage, err = getSwapMemory()
	if err != nil {
		return usage, err
	}

	usage.bandwidthIn.usage, usage.bandwidthOut.usage, err = getBindwidthIO()
	if err != nil {
		return usage, err
	}

	return usage, nil
}

func getCpu() (float64, error) {
	percent, err := cpu.Percent(0, false)
	if err != nil {
		return -1, err
	}
	return percent[0], nil
}

func getVirtualMemory() (float64, error) {
	vm, err := mem.VirtualMemory()
	if err != nil {
		return -1, err
	}
	return vm.UsedPercent, nil
}

func getSwapMemory() (float64, error) {
	sm, err := mem.SwapMemory()
	if err != nil {
		return -1, err
	}
	return sm.UsedPercent, nil
}

func getBindwidthIO() (in float64, out float64, err error) {
	info, err := net.IOCounters(false)
	if err != nil {
		return -1, -1, err
	}
	return float64(info[0].BytesRecv), float64(info[0].BytesSent), nil
}
