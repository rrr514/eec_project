//
//  Scheduler.cpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//

#define MAX_TASKS_PER_VM 10
#define MAX_VM_PER_MACHINE 100

using namespace std;

#include "Scheduler.hpp"
#include <set>
#include <algorithm>
#include <cassert>

static bool migrating = false;
static unsigned active_machines;

class Compare {
public:
    bool operator() (const MachineStatus* a, const MachineStatus* b) const {
        MachineId_t a_id = a->id;
        MachineId_t b_id = b->id;
        MachineInfo_t a_info = Machine_GetInfo(a_id);
        MachineInfo_t b_info = Machine_GetInfo(b_id);
        unsigned a_mips = a_info.performance[0];
        unsigned b_mips = b_info.performance[0];
        unsigned a_power = a_info.c_states[0];
        unsigned b_power = b_info.c_states[0];
        unsigned a_efficiency = a_mips / a_power;
        unsigned b_efficiency = b_mips / b_power;
        MachineState_t a_state = a_info.s_state;
        MachineState_t b_state = b_info.s_state;

        if(a_state != b_state){
            return a_state < b_state; // Lower state first.
        }
        if (a_efficiency != b_efficiency)
            return a_efficiency > b_efficiency; // Most efficient first.
        if (a->vms.size() != b->vms.size())
            return a->vms.size() < b->vms.size(); // Lower utilization as a tiebreaker.
        return a->id < b->id;
    }
};

// List of servers sorted by their energy efficiency, most efficient to least efficient
// set<MachineStatus, Compare> machine_status;
vector<MachineStatus*> machine_status;
vector<bool> isVMMigrating;
unsigned num_active_machines; // S0
unsigned num_inactive_machines;

void Scheduler::Init() {
    // Find the parameters of the clusters
    // Get the total number of machines
    // For each machine:
    //      Get the type of the machine
    //      Get the memory of the machine
    //      Get the number of CPUs
    //      Get if there is a GPU or not
    // 
    SimOutput("Scheduler::Init(): Total number of machines is " + to_string(Machine_GetTotal()), 3);
    active_machines = Machine_GetTotal();
    SimOutput("Scheduler::Init(): Total number of active machines is " + to_string(active_machines), 3);

    SimOutput("Scheduler::Init(): Initializing scheduler", 1);
    for(unsigned i = 0; i < active_machines; i++) {
        machines.push_back(MachineId_t(i));
        MachineStatus* machine = new MachineStatus();
        machine->id = MachineId_t(i);
        machine->vms = {};
        machine_status.push_back(machine);
    }
    
    // // Turn off all machines
    // for(MachineId_t machine: machines){
    //     Machine_SetState(machine, S5);
    // }

    num_active_machines = Machine_GetTotal();
    num_inactive_machines = 0;

    // // Print out all machine states
    // for(MachineId_t machine: machines){
    //     MachineInfo_t info = Machine_GetInfo(machine);
    //     SimOutput("Machine " + to_string(machine) + " state: " + to_string(info.s_state), 3);
    // }
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    // Update your data structure. The VM now can receive new tasks
    SimOutput("Scheduler::MigrationComplete(): Migration of VM " + to_string(vm_id) + " is complete at " + to_string(time), 3);
    migrating = false;
    isVMMigrating[vm_id] = false;
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    // Get the task parameters
    // TaskInfo_t task_info = GetTaskInfo(task_id);
    //  IsGPUCapable(task_id);
    // bool task_gpu_capable = IsTaskGPUCapable(task_id);
    //  GetMemory(task_id);
    unsigned task_required_memory = GetTaskMemory(task_id);
    //  RequiredVMType(task_id);
    VMType_t task_required_vm_type = RequiredVMType(task_id);
    //  RequiredSLA(task_id);
    // SLAType_t task_required_sla = RequiredSLA(task_id);
    //  RequiredCPUType(task_id);
    CPUType_t task_required_cpu = RequiredCPUType(task_id);

    // Decide to attach the task to an existing VM, 
    //      vm.AddTask(taskid, Priority_T priority); or

    // Intiailize all tasks with low priority
	Priority_t priority = LOW_PRIORITY;

    // Sort machines by efficiency
    sort(machine_status.begin(), machine_status.end(), Compare());

    // // Print out machine state, efficiency, utilization, and id
    // for(MachineStatus* machine: machine_status){
    //     MachineInfo_t info = Machine_GetInfo(machine->id);
    //     SimOutput("Machine " + to_string(machine->id) + " state: " + to_string(info.s_state), 3);
    //     SimOutput("Machine " + to_string(machine->id) + " efficiency: " + to_string(info.performance[0] / info.c_states[0]), 3);
    //     SimOutput("Machine " + to_string(machine->id) + " utilization: " + to_string(machine->vms.size()), 3);
    //     SimOutput("Machine " + to_string(machine->id) + " id: " + to_string(machine->id), 3);
    // }

    // Look for a valid machine
    for(MachineStatus* machine: machine_status) {
        MachineInfo_t info = Machine_GetInfo(machine->id);

        if(info.cpu != task_required_cpu) continue;
        if(info.memory_used + task_required_memory > info.memory_size) continue;
        // if(info.gpus && !task_gpu_capable) continue;

        bool added = false;
        // SimOutput("Machines size: " + to_string(machines.size()), 3);
        // SimOutput("VMs size: " + to_string(vms.size()), 3);
        // Look through VM list to see if task can be added to any existing VMs
        for(unsigned i = 0;i < machine->vms.size();i++){
            if(canRunTask(machine->vms[i], task_id)){
                SimOutput("Adding task " + to_string(task_id) + " to VM " + to_string(machine->vms[i]) + " on machine " + to_string(machine->id) + " at " + to_string(now), 3);
                assert(!isVMMigrating[machine->vms[i]]);
                VM_AddTask(machine->vms[i], task_id, priority);
                SimOutput("Successfully added task " + to_string(task_id) + " to VM " + to_string(machine->vms[i]) + " on machine " + to_string(machine->id) + " at " + to_string(now), 3);
                added = true;
                VMInfo_t vm_info = VM_GetInfo(machine->vms[i]);
                // vm_info.active_tasks.push_back(task_id);
                SimOutput("VM utilizatoin: " + to_string(vm_info.active_tasks.size()), 3);
                // Print out tasks in VM
                for(TaskId_t task: vm_info.active_tasks){
                    SimOutput("Task in VM: " + to_string(task), 3);
                }
                SimOutput("Machine utilization: " + to_string(machine->vms.size()), 3);
                return;
            }
        }
        
        // If not, create a new VM and attach the VM to a machine
        if(!added){
            if(canAttachVM(machine)){
                VMId_t vm_new = VM_Create(task_required_vm_type, task_required_cpu);
                isVMMigrating.push_back(false);
                VM_Attach(vm_new, machine->id);
                SimOutput("Attached VM " + to_string(vm_new) + " to machine " + to_string(machine->id) + " at " + to_string(now), 3);
                SimOutput("Attached VM of type " + to_string(task_required_vm_type) + " to machine of type " + to_string(task_required_cpu), 3);
                VM_AddTask(vm_new, task_id, priority);
                SimOutput("Added task " + to_string(task_id) + " to VM " + to_string(vm_new) + " at " + to_string(now), 3);
                machine->vms.push_back(vm_new);
                VMInfo_t vm_info = VM_GetInfo(vm_new);
                // vm_info.active_tasks.push_back(task_id);
                SimOutput("VM utilization: " + to_string(vm_info.active_tasks.size()), 3);
                // Print out tasks in VM
                for(TaskId_t task: vm_info.active_tasks){
                    SimOutput("Task in VM: " + to_string(task), 3);
                }

                SimOutput("Machine utilization: " + to_string(machine->vms.size()), 3);
                MachineInfo_t machine_info = Machine_GetInfo(machine->id);
                assert(machine_info.memory_used < machine_info.memory_size);
                return;
            }
        }

        
    }

    // TODO: if no valid machine, wake one up

    SimOutput("Warning! Task missed at " + to_string(now), 3);


    // Create a new VM, attach the VM to a machine
    //      VM vm(type of the VM)
    //      vm.Attach(machine_id);
    //      vm.AddTask(taskid, Priority_t priority) or
    // Turn on a machine, create a new VM, attach it to the VM, then add the task
    //
    // Turn on a machine, migrate an existing VM from a loaded machine....
    //
    // Other possibilities as desired
    // Priority_t priority = (task_id == 0 || task_id == 64)? HIGH_PRIORITY : MID_PRIORITY;
    // if(migrating) {
    //     VM_AddTask(vms[0], task_id, priority);
    // }
    // else {
    //     VM_AddTask(vms[task_id % active_machines], task_id, priority);
    //}
    // Skeleton code, you need to change it according to your algorithm
}

void Scheduler::PeriodicCheck(Time_t now) {
    // This method should be called from SchedulerCheck()
    // SchedulerCheck is called periodically by the simulator to allow you to monitor, make decisions, adjustments, etc.
    // Unlike the other invocations of the scheduler, this one doesn't report any specific event
    // Recommendation: Take advantage of this function to do some monitoring and adjustments as necessary

    // Iterate through all tasks and change priorities as necessary:
    // 1. If a task has less than 20% of its run time left, set priority to high
    // 2. If a task has less than 50% of its run time left, set priority to mid
    // 3. If a task has more than 50% of its run time left, set priority to low
    // A better way to assign priorities would probably be to use a combination of remaining instructions and time left, but this is simpler.
    double high_priority_threshold = 0.2;
    double mid_priority_threshold = 0.5;
    for(MachineStatus* machine : machine_status){
        for(VMId_t vm: machine->vms){
            VMInfo_t vm_info = VM_GetInfo(vm);
            for(TaskId_t task: vm_info.active_tasks){
                TaskInfo_t task_info = GetTaskInfo(task);
                Time_t total_time_given = task_info.target_completion - task_info.arrival;
                Time_t total_time_elapsed = now - task_info.arrival;
                double percent_time_elapsed = (double)total_time_elapsed / total_time_given;
                double percent_time_remaining = 1 - percent_time_elapsed;
                if(percent_time_remaining < high_priority_threshold){
                    SetTaskPriority(task, HIGH_PRIORITY);
                }
                else if(percent_time_remaining < mid_priority_threshold){
                    SetTaskPriority(task, MID_PRIORITY);
                }
                else{
                    SetTaskPriority(task, LOW_PRIORITY);
                }
            }
        }
    }


    // This priority setting scheme more complicated and doesn't work as well for (SpikeyMean and MatchMeIfYouCan and BigSmall)
    // Works better on TallShort though

    // Iterate through all tasks and change priorities as based on progress relative to time:
    // Let pc (percent_instructions_completed) - the percentage of the task instructions that are completed
    // Let pt (percent_time_remaining) - the percentage of the task time remaining
    // 1. If pc < pt or has already violated SLA, set priority to high
    // 2. If pc > pt, set priority to mid
    // 3. If task is SLA3, set priority to low
    // for(MachineStatus* machine : machine_status){
    //     for(VMId_t vm: machine->vms){
    //         VMInfo_t vm_info = VM_GetInfo(vm);
    //         for(TaskId_t task: vm_info.active_tasks){
    //             TaskInfo_t task_info = GetTaskInfo(task);
    //             uint64_t instructions_completed = task_info.total_instructions - task_info.remaining_instructions;
    //             double percent_instructions_completed = (double)instructions_completed / task_info.total_instructions;
    //             Time_t total_time_given = task_info.target_completion - task_info.arrival;
    //             Time_t total_time_elapsed = now - task_info.arrival;
    //             assert(total_time_given != 0);
    //             double percent_time_elapsed = (double)total_time_elapsed / total_time_given;
    //             double percent_time_remaining = 1 - percent_time_elapsed;
    //             if(IsSLAViolation(task) || percent_instructions_completed < percent_time_remaining){
    //                 SetTaskPriority(task, HIGH_PRIORITY);
    //             }
    //             else if(percent_instructions_completed > percent_time_remaining){
    //                 SetTaskPriority(task, MID_PRIORITY);
    //             }
    //             else if(task_info.required_sla == SLA3){
    //                 SetTaskPriority(task, LOW_PRIORITY);
    //             }
    //         }
    //     }
    // }
}

void Scheduler::Shutdown(Time_t time) {
    // Do your final reporting and bookkeeping here.
    // Report about the total energy consumed
    // Report about the SLA compliance
    // Shutdown everything to be tidy :-)
    for(auto & vm: vms) {
        VM_Shutdown(vm);
    }
    SimOutput("SimulationComplete(): Finished!", 4);
    SimOutput("SimulationComplete(): Time is " + to_string(time), 4);
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    // Do any bookkeeping necessary for the data structures
    // Decide if a machine is to be turned off, slowed down, or VMs to be migrated according to your policy
    // This is an opportunity to make any adjustments to optimize performance/energy
    SimOutput("Scheduler::TaskComplete(): Task " + to_string(task_id) + " is complete at " + to_string(now), 2);

    SimOutput("Migrating ARM VMs to higher efficiency machines", 4);
    migrateVMsToHigherEfficiencyMachines(ARM);
    SimOutput("Successfully migrated ARM VMs to higher efficiency machines", 4);
    SimOutput("Migrating X86 VMs to higher efficiency machines", 4);
    migrateVMsToHigherEfficiencyMachines(X86);
    SimOutput("Successfully migrated X86 VMs to higher efficiency machines", 4);
    SimOutput("Migrating POWER VMs to higher efficiency machines", 4);
    migrateVMsToHigherEfficiencyMachines(POWER);
    SimOutput("Successfully migrated POWER VMs to higher efficiency machines", 4);
    SimOutput("Migrating RISCV VMs to higher efficiency machines", 4);
    migrateVMsToHigherEfficiencyMachines(RISCV);
    SimOutput("Successfully migrated RISCV VMs to higher efficiency machines", 4);
}

// Public interface below

static Scheduler Scheduler;

void InitScheduler() {
    SimOutput("InitScheduler(): Initializing scheduler", 4);
    Scheduler.Init();
}

void HandleNewTask(Time_t time, TaskId_t task_id) {
    SimOutput("HandleNewTask(): Received new task " + to_string(task_id) + " at time " + to_string(time), 4);
    Scheduler.NewTask(time, task_id);
}

void HandleTaskCompletion(Time_t time, TaskId_t task_id) {
    SimOutput("HandleTaskCompletion(): Task " + to_string(task_id) + " completed at time " + to_string(time), 4);
    Scheduler.TaskComplete(time, task_id);
}

void MemoryWarning(Time_t time, MachineId_t machine_id) {
    // The simulator is alerting you that machine identified by machine_id is overcommitted
    SimOutput("MemoryWarning(): Overflow at " + to_string(machine_id) + " was detected at time " + to_string(time), 0);
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    // The function is called on to alert you that migration is complete
    SimOutput("MigrationDone(): Migration of VM " + to_string(vm_id) + " was completed at time " + to_string(time), 4);
    Scheduler.MigrationComplete(time, vm_id);
}

void SchedulerCheck(Time_t time) {
    // This function is called periodically by the simulator, no specific event
    SimOutput("SchedulerCheck(): SchedulerCheck() called at " + to_string(time), 4);
    Scheduler.PeriodicCheck(time);
    // static unsigned counts = 0;
    // counts++;
    // if(counts == 10) {
    //     migrating = true;
    //     VM_Migrate(1, 9);
    // }
}

void SimulationComplete(Time_t time) {
    // This function is called before the simulation terminates Add whatever you feel like.
    cout << "SLA violation report" << endl;
    cout << "SLA0: " << GetSLAReport(SLA0) << "%" << endl;
    cout << "SLA1: " << GetSLAReport(SLA1) << "%" << endl;
    cout << "SLA2: " << GetSLAReport(SLA2) << "%" << endl;     // SLA3 do not have SLA violation issues
    cout << "Total Energy " << Machine_GetClusterEnergy() << "KW-Hour" << endl;
    cout << "Simulation run finished in " << double(time)/1000000 << " seconds" << endl;
    SimOutput("SimulationComplete(): Simulation finished at time " + to_string(time), 4);
    
    Scheduler.Shutdown(time);
}

void SLAWarning(Time_t time, TaskId_t task_id) {
    
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    // Called in response to an earlier request to change the state of a machine
    SimOutput("StateChangeComplete(): State change of machine " + to_string(machine_id) + " completed at time " + to_string(time), 4);
}

bool canRunTask(VMId_t vm, TaskId_t task_id){
    VMInfo_t vm_info = VM_GetInfo(vm);
    // TaskInfo_t task_info = GetTaskInfo(task_id);
    bool correct_vm_type = vm_info.vm_type == RequiredVMType(task_id);
    bool correct_cpu = vm_info.cpu == RequiredCPUType(task_id);
    bool isOverloaded = vm_info.active_tasks.size() >= MAX_TASKS_PER_VM;
    bool isMigrating = isVMMigrating[vm];
    return correct_vm_type && correct_cpu && !isOverloaded && !isMigrating;
}

bool canAttachVM(MachineStatus* machine){
    MachineInfo_t machine_info = Machine_GetInfo(machine->id);
    bool isOverloaded = machine->vms.size() >= MAX_VM_PER_MACHINE;
    bool enoughMemory = machine_info.memory_size - machine_info.memory_used > 8; // ~8 MBs needed per VM
    return !isOverloaded && enoughMemory;
}

void migrateVMsToHigherEfficiencyMachines(CPUType_t cpuType){
    // Sort machines by efficiency
    sort(machine_status.begin(), machine_status.end(), Compare());

    // Divide servers into two halves based on utilization AND machine efficiency
    vector<MachineStatus*> low_efficiency_machines;
    vector<MachineStatus*> high_efficiency_machines;

    unsigned numMachines = 0;
    for(MachineStatus* machine: machine_status){
        MachineInfo_t info = Machine_GetInfo(machine->id);
        if(info.cpu == cpuType){
            numMachines++;
        }
    }

    unsigned i = 0;
    for(MachineStatus* machine: machine_status) {
        MachineInfo_t info = Machine_GetInfo(machine->id);
        if(info.cpu != cpuType) continue;

        if(i < numMachines / 2) {
            high_efficiency_machines.push_back(machine);
        }
        else {
            low_efficiency_machines.push_back(machine);
        }
        i++;
    }

    // Find smallest workload in least utilized and efficienct server
    // Migrate the workload to one of the highly utilized and efficient servers
    // Repeat until no more to migrate
    int currHighEfficiencyMachine = 0;
    int currLowEfficiencyMachine = low_efficiency_machines.size() - 1;
    while(currLowEfficiencyMachine >= 0) {
        // Find the next high efficiency machine that is not full
        while(currHighEfficiencyMachine < (int) high_efficiency_machines.size() && high_efficiency_machines[currHighEfficiencyMachine]->vms.size() == MAX_VM_PER_MACHINE) {
            currHighEfficiencyMachine++;
        }
        if(currHighEfficiencyMachine >= (int) high_efficiency_machines.size()) {
            break;
        }

        MachineStatus* le_machine = low_efficiency_machines[currLowEfficiencyMachine];
        MachineStatus* he_machine = high_efficiency_machines[currHighEfficiencyMachine];

        // Migrate VMs from le_machine to he_machine (skip VMs that are already migrating)
        bool foundValid = false;
        while(le_machine->vms.size() > 0 && he_machine->vms.size() < MAX_VM_PER_MACHINE) {
            int idx = (int)le_machine->vms.size() - 1;
            foundValid = false;
            // Search for a VM that is not in the process of migrating.
            while(idx >= 0) {
                VMId_t candidate = le_machine->vms[idx];
                if(isMigratableVM(candidate)){
                    foundValid = true;
                    break;
                }
                idx--;
            }
            if(!foundValid) {
                SimOutput("No valid VMs available to migrate on this machine.", 3);
                break; 
            }

            VMId_t vmToMigrate = le_machine->vms[idx];
            // Erase the VM from low efficiency machine.
            le_machine->vms.erase(le_machine->vms.begin() + idx);

            isVMMigrating[vmToMigrate] = true;
            SimOutput("Migrating VM " + to_string(vmToMigrate) + " from machine " +
                      to_string(le_machine->id) + " to machine " + to_string(he_machine->id), 3);
            VM_Migrate(vmToMigrate, he_machine->id);
            he_machine->vms.push_back(vmToMigrate);

            SimOutput("Low Efficiency Machine VM size: " + to_string(le_machine->vms.size()), 3);
            // Print out the VMs in the low efficiency machine
            for(VMId_t vm: le_machine->vms){
                SimOutput("VM in low efficiency machine: " + to_string(vm), 3);
            }
            SimOutput("High Efficiency Machine VM size: " + to_string(he_machine->vms.size()), 3);
            // Print out the VMs in the high efficiency machine
            for(VMId_t vm: he_machine->vms){
                SimOutput("VM in high efficiency machine: " + to_string(vm), 3);
            }
        }

        low_efficiency_machines[currLowEfficiencyMachine] = le_machine;
        high_efficiency_machines[currHighEfficiencyMachine] = he_machine;
        
        // If the low efficiency machine is empty, move to the next one
        if(!foundValid) {
            currLowEfficiencyMachine--;
        }
    }
}

Time_t computeVMRemainingRunTime(VMId_t vm_id){
    // Get total active task remaining instructions
    VMInfo_t vm_info = VM_GetInfo(vm_id);
    uint64_t totalRemainingInstructions = 0;
    for(TaskId_t task_id: vm_info.active_tasks){
        TaskInfo_t task_info = GetTaskInfo(task_id);
        totalRemainingInstructions += task_info.remaining_instructions;
    }
    // Get machine MIPS
    MachineId_t machine_id = vm_info.machine_id;
    MachineInfo_t machine_info = Machine_GetInfo(machine_id);
    unsigned machineMIPS = machine_info.performance[0];
    // Compute reminaing run time
    Time_t remainingRunTime = totalRemainingInstructions / machineMIPS;
    return remainingRunTime;
}

bool isMigratableVM(VMId_t vm_id){
    bool isCurrentlyMigrating = isVMMigrating[vm_id];
    Time_t remainingRunTime = computeVMRemainingRunTime(vm_id);
    Time_t fifteenMinutes = 15 * 60 * 1000000; // 15 minutes in microseconds
    bool hasMoreThanFifteenMinutesOfTaskRunTimeLeft = remainingRunTime > fifteenMinutes;
    return !isCurrentlyMigrating && hasMoreThanFifteenMinutesOfTaskRunTimeLeft;
}

MachineState_t get_machine_s_state(MachineId_t machine_id){
    MachineInfo_t machine_info = Machine_GetInfo(machine_id);
    return machine_info.s_state;
}
