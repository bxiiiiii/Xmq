package collect

import (
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/mem"
	"github.com/shirou/gopsutil/net"
)

type BrokerUsage struct {
	Cpu           ResourceUsage
	VirtualMemory ResourceUsage
	SwapMemory    ResourceUsage
	BandwidthIn   ResourceUsage
	BandwidthOut  ResourceUsage
}

type ResourceUsage struct {
	Usage float64
	Limit float64
}

func CollectLoadData() (Usage BrokerUsage, err error) {
	Usage.Cpu.Usage, err = getCpu()
	if err != nil {
		return Usage, err
	}

	Usage.VirtualMemory.Usage, err = getVirtualMemory()
	if err != nil {
		return Usage, err
	}

	Usage.SwapMemory.Usage, err = getSwapMemory()
	if err != nil {
		return Usage, err
	}

	Usage.BandwidthIn.Usage, Usage.BandwidthOut.Usage, err = getBindwidthIO()
	if err != nil {
		return Usage, err
	}

	return Usage, nil
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
