# Basically, have a bunch of differing tasks and figure out how to schedule them for optimized performance
# These are powerful machines, so priority is on efficient scheduling
machine class:
{
       Number of machines: 32
       CPU type: X86
       Number of cores: 16
       Memory: 32768
       S-States: [120, 100, 80, 40, 10, 0]
       P-States: [15, 12, 8, 4]
       C-States: [15, 8, 4, 0]
       MIPS: [2000, 1600, 1200, 800]
       GPUs: yes
}


machine class:
{
       Number of machines: 32
       CPU type: ARM
       Number of cores: 32
       Memory: 32768
       S-States: [120, 100, 80, 60, 40, 10, 0]
       P-States: [12, 8, 6, 4]
       C-States: [12, 6, 3, 0]
       MIPS: [1800, 1400, 1000, 600]
       GPUs: yes
}


# Web Requests should be completed quickly by SLA
task class:
{
       Start time: 0
       End time: 1000000
       Inter arrival: 100
       Expected runtime: 50000
       Memory: 4096
       VM type: LINUX
       GPU enabled: no
       SLA type: SLA0
       CPU type: X86
       Task type: WEB
       Seed: 123456
}


# High-Intensity AI workload
task class:
{
       Start time: 100000
       End time: 2000000
       Inter arrival: 5000
       Expected runtime: 500000
       Memory: 8192
       VM type: LINUX
       GPU enabled: yes
       SLA type: SLA1
       CPU type: X86
       Task type: AI
       Seed: 789012
}


# High-Intensity Crypto - Need to juggle both but Crypto should be handled first before AI from SLA
task class:
{
       Start time: 300000
       End time: 800000
       Inter arrival: 500
       Expected runtime: 100000
       Memory: 2048
       VM type: LINUX
       GPU enabled: yes
       SLA type: SLA0
       CPU type: X86
       Task type: CRYPTO
       Seed: 901234
}


# High-Intensity Memory for ARM
task class:
{
       Start time: 50000
       End time: 1500000
       Inter arrival: 2000
       Expected runtime: 300000
       Memory: 16384
       VM type: LINUX
       GPU enabled: no
       SLA type: SLA2
       CPU type: ARM
       Task type: HPC
       Seed: 345678
}


# High-Intensity CPU also for ARM - Must handle alongside the Memory
task class: {
       Start time: 200000
       End time: 1800000
       Inter arrival: 1000
       Expected runtime: 400000
       Memory: 8192
       VM type: LINUX
       GPU enabled: yes
       SLA type: SLA1
       CPU type: ARM
       Task type: AI
       Seed: 678901
}


# Long-running Streaming - Basically a background task to juggle
task class:
{
       Start time: 0
       End time: 2000000
       Inter arrival: 20000
       Expected runtime: 1000000
       Memory: 4096
       VM type: LINUX
       GPU enabled: no
       SLA type: SLA2
       CPU type: ARM
       Task type: STREAM
       Seed: 567890
}


