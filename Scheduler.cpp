//
//  Scheduler.cpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//

#define MAX_TASKS_PER_VM 10
#define MAX_VM_PER_MACHINE 10

using namespace std;

#include "Scheduler.hpp"
#include <set>
#include <algorithm>
#include <cassert>
#include <queue>
#include <unordered_map>

static bool migrating = false;
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

set<MachineStatus*, Compare> machine_status;
vector<bool> isVMMigrating;
vector<bool> isMachineChangingState;
// static unsigned num_active_machines; // S0
// static unsigned num_inactive_machines;
unsigned total_machines;
queue<TaskId_t> missed_tasks;
uint64_t numCycles = 0;
unordered_map<MachineId_t, Time_t> machine_standby_time;

void Scheduler::Init() {
    // Find the parameters of the clusters
    // Get the total number of machines
    // For each machine:
    //      Get the type of the machine
    //      Get the memory of the machine
    //      Get the number of CPUs
    //      Get if there is a GPU or not
    // 
    total_machines = Machine_GetTotal();
    SimOutput("Scheduler::Init(): Total number of machines is " + to_string(total_machines), 3);
    SimOutput("Scheduler::Init(): Initializing scheduler", 1);
    for(unsigned i = 0; i < total_machines; i++) {
        machines.push_back(MachineId_t(i));
        MachineStatus* machine = new MachineStatus();
        machine->id = MachineId_t(i);
        machine->vms = {};
        // Originally: machine_status.push_back(machine);
        // Now, insert into the ordered set.
        machine_status.insert(machine);
        isMachineChangingState.push_back(false);
    }

    // Print out machines
    // for(MachineStatus* machine: machine_status){
    //     MachineInfo_t info = Machine_GetInfo(machine->id);
    //     SimOutput("Machine " + to_string(machine->id) + " state: " + to_string(info.s_state), 3);
    //     SimOutput("Machine " + to_string(machine->id) + " efficiency: " + to_string(info.performance[0] / info.c_states[0]), 3);
    //     SimOutput("Machine " + to_string(machine->id) + " utilization: " + to_string(machine->vms.size()), 3);
    //     SimOutput("Machine " + to_string(machine->id) + " id: " + to_string(machine->id), 3);
    // }
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    // Update your data structure. The VM now can receive new tasks
    SimOutput("Scheduler::MigrationComplete(): Migration of VM " + to_string(vm_id) + " is complete at " + to_string(time), 3);
    migrating = false;
    isVMMigrating[vm_id] = false;
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    SimOutput("Scheduler::NewTask(): Received new task " + to_string(task_id) + " at time " + to_string(now), 3);
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

    // Initialize all tasks with low priority
    Priority_t priority = LOW_PRIORITY;

    // Originally: sort(machine_status.begin(), machine_status.end(), Compare());
    // Since machine_status is an ordered set, sorting is unnecessary.
    // (The sort call is preserved here as a comment.)
    // sort(machine_status.begin(), machine_status.end(), Compare());

    // Print out machine state, efficiency, utilization, and id
    // for(MachineStatus* machine: machine_status){
    //     MachineInfo_t info = Machine_GetInfo(machine->id);
    //     SimOutput("Machine " + to_string(machine->id) + " state: " + to_string(info.s_state), 3);
    //     SimOutput("Machine " + to_string(machine->id) + " efficiency: " + to_string(info.performance[0] / info.c_states[0]), 3);
    //     SimOutput("Machine " + to_string(machine->id) + " utilization: " + to_string(machine->vms.size()), 3);
    //     SimOutput("Machine " + to_string(machine->id) + " id: " + to_string(machine->id), 3);
    // }

    bool success = scheduleNewTask(task_required_cpu, task_required_vm_type, task_id, priority, now, task_required_memory);

    // No more machines to wake up --> add to missed task queue
    if(!success){
        // SimOutput("Task missed at " + to_string(now), 3);
        missed_tasks.push(task_id);
        // SimOutput("Task " + to_string(task_id) + " is added to the missed task queue", 3);
    }

    scheduleMissedTasks(now);
}

void Scheduler::PeriodicCheck(Time_t now) {
    // SimOutput("Scheduler::PeriodicCheck(): Periodic check at time " + to_string(now), 3);
    numCycles++;
    // This method should be called from SchedulerCheck()
    // SchedulerCheck is called periodically by the simulator to allow you to monitor, make decisions, adjustments, etc.
    // Unlike the other invocations of the scheduler, this one doesn't report any specific event
    // Recommendation: Take advantage of this function to do some monitoring and adjustments as necessary

    // Iterate through all tasks and change priorities as necessary:
    // 1. If a task has less than 20% of its run time left, set priority to high
    // 2. If a task has less than 50% of its run time left, set priority to mid
    // 3. If a task has more than 50% of its run time left, set priority to low
    double high_priority_threshold = 0.2;
    double mid_priority_threshold = 0.5;
    for(auto machine : machine_status) {
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
    // for(auto machine : machine_status){
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

    // Try to schedule missed tasks if possible
    scheduleMissedTasks(now);

    // Attempt to lower the frequency of the machines
    if(missed_tasks.empty()){
        // SimOutput("No missed tasks, attempting to lower frequency of machines", 3);
        for(auto machine : machine_status){
            MachineInfo_t info = Machine_GetInfo(machine->id);
            if(isMachineChangingState[machine->id]) continue;
            if(get_machine_s_state(machine->id) != S0) continue;
            assert(info.s_state == S0);
            changeMachineFrequency(machine, now);
        }
    }
    else{
        // SimOutput("Missed tasks present, not lowering frequency of machines", 3);
        assert(missed_tasks.size() > 0);
    }

    // If on standby for more than 5 minutes, turn off the machine
    for(auto machine : machine_status){
        MachineInfo_t info = Machine_GetInfo(machine->id);
        if(info.s_state == S1 && isMachineChangingState[machine->id] == false){
            assert(info.active_tasks == 0);
            assert(info.active_vms == 0);
            Time_t time_on_standby = now - machine_standby_time[machine->id];
            if(time_on_standby > 300000000){
                // SimOutput("Turning off machine " + to_string(machine->id) + " at " + to_string(now), 3);
                isMachineChangingState[machine->id] = true;
                Machine_SetState(machine->id, S5);
            }
        }
    }
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
    // Scheduler.Shutdown(time);
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    // Do any bookkeeping necessary for the data structures
    // Decide if a machine is to be turned off, slowed down, or VMs to be migrated according to your policy
    // This is an opportunity to make any adjustments to optimize performance/energy
    SimOutput("Scheduler::TaskComplete(): Task " + to_string(task_id) + " is complete at " + to_string(now), 2);

    // SimOutput("Migrating ARM VMs to higher efficiency machines", 4);
    migrateVMsToHigherEfficiencyMachines(ARM);
    // SimOutput("Successfully migrated ARM VMs to higher efficiency machines", 4);
    // SimOutput("Migrating X86 VMs to higher efficiency machines", 4);
    migrateVMsToHigherEfficiencyMachines(X86);
    // SimOutput("Successfully migrated X86 VMs to higher efficiency machines", 4);
    // SimOutput("Migrating POWER VMs to higher efficiency machines", 4);
    migrateVMsToHigherEfficiencyMachines(POWER);
    // SimOutput("Successfully migrated POWER VMs to higher efficiency machines", 4);
    // SimOutput("Migrating RISCV VMs to higher efficiency machines", 4);
    migrateVMsToHigherEfficiencyMachines(RISCV);
    // SimOutput("Successfully migrated RISCV VMs to higher efficiency machines", 4);

    // Set unused machines on standby
    for(auto machine : machine_status){
        MachineInfo_t info = Machine_GetInfo(machine->id);
        if(info.s_state == S0 && machine->vms.empty() && isMachineChangingState[machine->id] == false){
            assert(info.active_tasks == 0);
            assert(info.active_vms == 0);
            isMachineChangingState[machine->id] = true;
            // num_active_machines--;
            // num_inactive_machines++;
            // SimOutput("Turning off machine " + to_string(machine->id) + " at " + to_string(now), 3);
            machine_standby_time[machine->id] = now;
            Machine_SetState(machine->id, S1);
        }
    }
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
    // Implement SLA warning logic here, if needed.
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    // Called in response to an earlier request to change the state of a machine
    SimOutput("StateChangeComplete(): State change of machine " + to_string(machine_id) + " completed at time " + to_string(time), 3);
    isMachineChangingState[machine_id] = false;
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
    bool isChangingState = isMachineChangingState[machine->id];
    return !isOverloaded && enoughMemory && !isChangingState;
}

void migrateVMsToHigherEfficiencyMachines(CPUType_t cpuType){
    // Because machine_status is a set, we first copy it to a vector to allow indexing and sort.
    vector<MachineStatus*> machineVec;
    for(auto m : machine_status) {
        machineVec.push_back(m);
    }
    // Sort the temporary vector using the Compare class.
    sort(machineVec.begin(), machineVec.end(), Compare());

    // Divide servers into two halves based on utilization AND machine efficiency
    vector<MachineStatus*> low_efficiency_machines;
    vector<MachineStatus*> high_efficiency_machines;

    unsigned numMachines = 0;
    for(MachineStatus* machine: machineVec){
        MachineInfo_t info = Machine_GetInfo(machine->id);
        if(info.cpu == cpuType){
            numMachines++;
        }
    }

    unsigned i = 0;
    for(MachineStatus* machine: machineVec) {
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

    // Find smallest workload in least utilized and efficient server.
    // Migrate the workload to one of the highly utilized and efficient servers.
    int currHighEfficiencyMachine = 0;
    int currLowEfficiencyMachine = low_efficiency_machines.size() - 1;
    while(currLowEfficiencyMachine >= 0) {
        // Find the next high efficiency machine that is not full.
        while(currHighEfficiencyMachine < (int) high_efficiency_machines.size() && 
              high_efficiency_machines[currHighEfficiencyMachine]->vms.size() == MAX_VM_PER_MACHINE) {
            currHighEfficiencyMachine++;
        }
        if(currHighEfficiencyMachine >= (int) high_efficiency_machines.size()) {
            break;
        }

        MachineStatus* le_machine = low_efficiency_machines[currLowEfficiencyMachine];
        MachineStatus* he_machine = high_efficiency_machines[currHighEfficiencyMachine];

        // Migrate VMs from le_machine to he_machine (skip VMs that are already migrating)
        bool foundValid = false;
        while(!le_machine->vms.empty() && he_machine->vms.size() < MAX_VM_PER_MACHINE) {
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
                // SimOutput("No valid VMs available to migrate on this machine.", 3);
                break; 
            }

            VMId_t vmToMigrate = le_machine->vms[idx];
            // Erase the VM from low efficiency machine.
            le_machine->vms.erase(le_machine->vms.begin() + idx);

            isVMMigrating[vmToMigrate] = true;
            // SimOutput("Migrating VM " + to_string(vmToMigrate) + " from machine " +
            //           to_string(le_machine->id) + " to machine " + to_string(he_machine->id), 3);
            VM_Migrate(vmToMigrate, he_machine->id);
            he_machine->vms.push_back(vmToMigrate);

            // SimOutput("Low Efficiency Machine VM size: " + to_string(le_machine->vms.size()), 3);
            // Print out the VMs in the low efficiency machine
            // for(VMId_t vm: le_machine->vms){
            //     // SimOutput("VM in low efficiency machine: " + to_string(vm), 3);
            // }
            // // SimOutput("High Efficiency Machine VM size: " + to_string(he_machine->vms.size()), 3);
            // // Print out the VMs in the high efficiency machine
            // for(VMId_t vm: he_machine->vms){
            //     // SimOutput("VM in high efficiency machine: " + to_string(vm), 3);
            // }
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
    // Compute remaining run time
    if(machineMIPS == 0){
        return INT64_MAX;
    }
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

// Returns true if the task was successfully added to a VM on the machine, false otherwise
bool addTaskToMachine(MachineStatus* machine, TaskId_t task_id, Priority_t priority, VMType_t task_required_vm_type, CPUType_t task_required_cpu, Time_t now){
    bool added = false;
    // SimOutput("Machines size: " + to_string(machines.size()), 3);
    // SimOutput("VMs size: " + to_string(vms.size()), 3);
    // Look through VM list to see if task can be added to any existing VMs
    for(unsigned i = 0; i < machine->vms.size(); i++){
        if(canRunTask(machine->vms[i], task_id)){
            // SimOutput("Adding task " + to_string(task_id) + " to VM " + to_string(machine->vms[i]) + " on machine " + to_string(machine->id) + " at " + to_string(now), 3);
            assert(!isVMMigrating[machine->vms[i]]);
            VM_AddTask(machine->vms[i], task_id, priority);
            // SimOutput("Successfully added task " + to_string(task_id) + " to VM " + to_string(machine->vms[i]) + " on machine " + to_string(machine->id) + " at " + to_string(now), 3);
            added = true;
            VMInfo_t vm_info = VM_GetInfo(machine->vms[i]);
            // vm_info.active_tasks.push_back(task_id);
            // SimOutput("VM utilizatoin: " + to_string(vm_info.active_tasks.size()), 3);
            // Print out tasks in VM
            // for(TaskId_t task: vm_info.active_tasks){
            //     SimOutput("Task in VM: " + to_string(task), 3);
            // }
            // SimOutput("Machine utilization: " + to_string(machine->vms.size()), 3);
            return true;
        }
    }
    
    // If not, create a new VM and attach the VM to a machine
    if(!added){
        if(canAttachVM(machine)){
            VMId_t vm_new = VM_Create(task_required_vm_type, task_required_cpu);
            isVMMigrating.push_back(false);
            VM_Attach(vm_new, machine->id);
            // SimOutput("Attached VM " + to_string(vm_new) + " to machine " + to_string(machine->id) + " at " + to_string(now), 3);
            // SimOutput("Attached VM of type " + to_string(task_required_vm_type) + " to machine of type " + to_string(task_required_cpu), 3);
            VM_AddTask(vm_new, task_id, priority);
            // SimOutput("Added task " + to_string(task_id) + " to VM " + to_string(vm_new) + " at " + to_string(now), 3);
            machine->vms.push_back(vm_new);
            VMInfo_t vm_info = VM_GetInfo(vm_new);
            // vm_info.active_tasks.push_back(task_id);
            // SimOutput("VM utilization: " + to_string(vm_info.active_tasks.size()), 3);
            // Print out tasks in VM
            // for(TaskId_t task: vm_info.active_tasks){
            //     SimOutput("Task in VM: " + to_string(task), 3);
            // }

            // SimOutput("Machine utilization: " + to_string(machine->vms.size()), 3);
            MachineInfo_t machine_info = Machine_GetInfo(machine->id);
            assert(machine_info.memory_used < machine_info.memory_size);
            return true;
        }
    }

    return false;
}

// Returns true if able to schedule the new task, false otherwise;
bool scheduleNewTask(CPUType_t task_required_cpu, VMType_t task_required_vm_type, TaskId_t task_id, Priority_t priority, Time_t now, unsigned task_required_memory){
    // Look for a valid machine.
    // Originally, this loop used an index on machine_status (a vector). We now iterate over the set.
    for(auto machine : machine_status) {
        MachineInfo_t info = Machine_GetInfo(machine->id);
        if(get_machine_s_state(machine->id) != S0) continue;
        assert(info.s_state == S0);

        if(info.cpu != task_required_cpu) continue;
        if(info.memory_used + task_required_memory > info.memory_size) continue;
        if(isMachineChangingState[machine->id]) {
            // SimOutput("Machine " + to_string(machine->id) + " is changing state, skipping", 3);
            continue;
        }
        bool success = addTaskToMachine(machine, task_id, priority, task_required_vm_type, task_required_cpu, now);
        if(success){
            return true;
        }
    }

    // If no valid machine, wake one up.
    for(auto machine : machine_status) {
        // Original code checked: if(get_machine_s_state(machine_to_wake->id) != S5 || get_machine_s_state(machine_to_wake->id) != S1) continue;
        // We preserve this check.
        if(get_machine_s_state(machine->id) != S5 && get_machine_s_state(machine->id) != S1) continue;
        assert(isMachineChangingState[machine->id] || get_machine_s_state(machine->id) == S5 || get_machine_s_state(machine->id) == S1);
        if(isMachineChangingState[machine->id]) {
            // SimOutput("Machine " + to_string(machine->id) + " is changing state, skipping", 3);
            continue;
        }
        isMachineChangingState[machine->id] = true;
        SimOutput("Waking up machine " + to_string(machine->id) + " at " + to_string(now), 3);
        Machine_SetState(machine->id, S0);
        // num_active_machines++;
        // num_inactive_machines--;

        bool success = addTaskToMachine(machine, task_id, priority, task_required_vm_type, task_required_cpu, now);
        if(success){
            return true;
        }
    }

    return false;
}

void scheduleMissedTasks(Time_t now){
    // SimOutput("Scheduler::scheduleMissedTasks(): Scheduling missed tasks at time " + to_string(now), 3);
    // Print out number of missed tasks
    // SimOutput("Scheduler::scheduleMissedTasks(): Number of initial missed tasks: " + to_string(missed_tasks.size()), 3);
    // Try to schedule missed tasks if possible
    queue<TaskId_t> still_missed_tasks;
    while(!missed_tasks.empty()){
        TaskId_t missed_task = missed_tasks.front();
        missed_tasks.pop();
        unsigned task_required_memory = GetTaskMemory(missed_task);
        VMType_t task_required_vm_type = RequiredVMType(missed_task);
        CPUType_t task_required_cpu = RequiredCPUType(missed_task);
        Priority_t priority = LOW_PRIORITY;
        // Try to schedule the task
        bool success = scheduleNewTask(task_required_cpu, task_required_vm_type, missed_task, priority, now, task_required_memory);
        if(!success){
            still_missed_tasks.push(missed_task);
        }
    }

    // Add the remaining missed tasks back to the queue
    while(!still_missed_tasks.empty()){
        TaskId_t missed_task = still_missed_tasks.front();
        still_missed_tasks.pop();
        missed_tasks.push(missed_task);
    }

    // Print out number of missed tasks remaining
    // SimOutput("Scheduler::scheduleMissedTasks(): Number of missed tasks remaining: " + to_string(missed_tasks.size()), 3);
}

void changeMachineFrequency(MachineStatus* machine, Time_t now){
    // Calculate the number of instructions remaining for all tasks on the machine
    int64_t total_remaining_instructions = 0;
    for(VMId_t vm: machine->vms){
        VMInfo_t vm_info = VM_GetInfo(vm);
        for(TaskId_t task: vm_info.active_tasks){
            TaskInfo_t task_info = GetTaskInfo(task);
            total_remaining_instructions += task_info.remaining_instructions;
        }
    }

    // Get the earliest deadline of all tasks on the machine
    int64_t earliest_deadline = INT64_MAX;
    for(VMId_t vm: machine->vms){
        VMInfo_t vm_info = VM_GetInfo(vm);
        for(TaskId_t task: vm_info.active_tasks){
            TaskInfo_t task_info = GetTaskInfo(task);
            // Print out task info
            // SimOutput("Task " + to_string(task) + " target completion: " + to_string(task_info.target_completion), 3);
            // SimOutput("Task " + to_string(task) + " arrival: " + to_string(task_info.arrival), 3);
            // SimOutput("Task " + to_string(task) + " remaining instructions: " + to_string(task_info.remaining_instructions), 3);
            // SimOutput("Task " + to_string(task) + " total instructions: " + to_string(task_info.total_instructions), 3);
            if(earliest_deadline == 0 || (int64_t) task_info.target_completion < earliest_deadline){
                earliest_deadline = task_info.target_completion;
            }
        }
    }

    // SimOutput("Machine " + to_string(machine->id) + " earliest deadline: " + to_string(earliest_deadline), 3);
    // SimOutput("Machine " + to_string(machine->id) + " now: " + to_string(now), 3);
    
    // For each MIPS state, determine whether the machine can complete in time
    MachineInfo_t machine_info = Machine_GetInfo(machine->id);
    int64_t remaining_time = earliest_deadline - now;
    if(remaining_time < 0){
        // Set to P0 if not already
        // SimOutput("Machine " + to_string(machine->id) + " is already past deadline, setting to P0", 3);
        MachineInfo_t machine_info = Machine_GetInfo(machine->id);
        if(machine_info.p_state != P0){
            // SimOutput("Machine " + to_string(machine->id) + " is not in P0, setting to P0", 3);
            // isMachineChangingState[machine->id] = true;
            Machine_SetCorePerformance(machine->id, 0, P0);
        }
        return;
    }

    vector<unsigned> performance = machine_info.performance;
    int p_state_int = P3;
    int p_state_to_change_to = P3;
    while(p_state_int >= 0){
        int64_t mips = performance[p_state_int];
        int64_t time_to_complete;
        if(mips == 0 && total_remaining_instructions == 0){
            time_to_complete = 0;
        }
        else if(mips == 0){
            continue;
        }
        else{
            time_to_complete = total_remaining_instructions / mips;
        }
        int64_t time_to_complete_with_buffer = time_to_complete * 2; // 10% buffer
        // SimOutput("Machine " + to_string(machine->id) + " remaining instructions: " + to_string(total_remaining_instructions), 3);
        // SimOutput("Machine " + to_string(machine->id) + " MIPS: " + to_string(mips), 3);
        // SimOutput("Machine " + to_string(machine->id) + " time to complete: " + to_string(time_to_complete), 3);
        // SimOutput("Machine " + to_string(machine->id) + " time to complete with buffer: " + to_string(time_to_complete_with_buffer), 3);
        // SimOutput("Machine " + to_string(machine->id) + " remaining time: " + to_string(remaining_time), 3);
        if(time_to_complete_with_buffer < remaining_time){
            p_state_to_change_to = p_state_int;
            // Print out MIPS + remaining instructions
            // SimOutput("Machine " + to_string(machine->id) + " can complete in time with P" + to_string(p_state_int) + " at time " + to_string(now), 3);
            break;
        }
        p_state_int--;
    }

    CPUPerformance_t desired_p_state = static_cast<CPUPerformance_t>(p_state_to_change_to);

    // If the machine is already in the desired state, do nothing
    if(desired_p_state == machine_info.p_state){
        // SimOutput("Machine " + to_string(machine->id) + " is already in the desired state, skipping", 3);
        return;
    }
    else{
        // Change the machine state
        // SimOutput("Changing machine " + to_string(machine->id) + " from state " + to_string(machine_info.p_state) + " to state " + to_string(p_state_to_change_to) + " at time " + to_string(now), 3);
        // isMachineChangingState[machine->id] = true;
        Machine_SetCorePerformance(machine->id, 0, desired_p_state);
    }
}
