//
//  Scheduler.cpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//

#include "Scheduler.hpp"
#include <map>
#include <algorithm>
#include <queue>
#include <set>

// static bool migrating = false;
// static unsigned active_machines = 16;
// static unsigned active_x86_machines = 8;
// static unsigned active_arm_machines = 8;
// static unsigned active_power_machines = 0;

// Implementation-specific types and data structures
namespace {
    struct MachineStatus {
        double utilization;
        vector<TaskId_t> tasks;
		vector<VMId_t> vms; // Track VMs on this machine
		map<VMId_t, vector<TaskId_t>> vm_tasks; // Track tasks on each VM
    };

    // Global state for scheduler implementation
    map<MachineId_t, MachineStatus> machine_status;
    map<TaskId_t, pair<VMId_t, MachineId_t>> task_locations;
    bool migrating = false;

	struct MachineUtilizationComparator {
		bool operator()(const MachineId_t& a, const MachineId_t& b) const {
			// Compare by utilization
			if (machine_status[a].utilization != machine_status[b].utilization)
				return machine_status[a].utilization > machine_status[b].utilization;

			// Check if either has a gpu while the other doesn't
			if (Machine_GetInfo(a).gpus != Machine_GetInfo(b).gpus)
				return Machine_GetInfo(a).gpus > Machine_GetInfo(b).gpus;

			// Otherwise, sort by memory used by each machine
			if (Machine_GetInfo(a).memory_used != Machine_GetInfo(b).memory_used)
				return Machine_GetInfo(a).memory_used > Machine_GetInfo(b).memory_used;
			
			// Tie-breaker by ID for consistent ordering
			return a < b;
		}
	};
	set<MachineId_t, MachineUtilizationComparator> active_machines_set; // For sorted access
	set<MachineId_t, MachineUtilizationComparator> inactive_machines_set; // For sorted access
}

// TODO: Helper functions
	// Moving tasks between machines
		// Specifically, going from a machine to more efficient machine

void Scheduler::Init() {
    SimOutput("Scheduler::Init(): Total number of machines is " + to_string(Machine_GetTotal()), 3);
    
    // Initialize all machines and VMs
    for (unsigned i = 0; i < Machine_GetTotal(); i++) {
        MachineInfo_t info = Machine_GetInfo(MachineId_t(i));
        machines.push_back(MachineId_t(i));
        
        // Initialize tracking structures for each machine
        machine_status[MachineId_t(i)] = {0.0, vector<TaskId_t>(), vector<VMId_t>(), map<VMId_t, vector<TaskId_t>>()};

		// Set initial state of machine to inactive and add to inactive machines
		// inactive_machines.push_back(MachineId_t(i));
		inactive_machines_set.insert(MachineId_t(i));
		Machine_SetState(MachineId_t(i), S5); // Power down all machines initially
    }
}

// When VM is finished moving into new machine
void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    migrating = false;
}

double calculateTaskUtilization(TaskId_t task_id, MachineId_t machine_id) {
    // Get task info
    TaskInfo_t task_info = GetTaskInfo(task_id);
    MachineInfo_t info = Machine_GetInfo(machine_id);
    
    // Machine capacity in instructions per second
    double machineInstructionsPerSec = info.performance[info.p_state] * 1000000.0 * info.num_cpus;
    
    // Task demand in instructions
    double taskInstructions = task_info.remaining_instructions;
    
    // Expected runtime in seconds
    double expectedRuntimeSec = task_info.target_completion - task_info.arrival;
    expectedRuntimeSec /= 1000000.0; // Convert from microseconds to seconds
    
    // Utilization is the fraction of the machine's capacity needed by this task
    double taskInstructionsPerSec = taskInstructions / expectedRuntimeSec;
    return taskInstructionsPerSec / machineInstructionsPerSec;
}

bool powerDownActiveMachine(MachineId_t machine_id) {
	// Power down the machine if it has no tasks
	if (machine_status[machine_id].tasks.empty()) {
		// SimOutput("Scheduler::powerDownActiveMachine(): Powering down machine " + to_string(machine_id), 0);
		Machine_SetState(machine_id, S5); // Power down
		active_machines_set.erase(machine_id); // Remove from active machines
		inactive_machines_set.insert(machine_id); // Move to inactive machines
		return true;
	}
	return false;
}

bool addTaskToMachineChecks(TaskId_t task_id, MachineId_t machine_id) {
	// 1. Find whether machine can support task
	MachineInfo_t info = Machine_GetInfo(machine_id);
	CPUType_t required_cpu = RequiredCPUType(task_id);
	unsigned memory = GetTaskMemory(task_id);

	if(info.cpu != required_cpu) return false;
	if(info.memory_used + memory > info.memory_size) return false;
	double new_utilization = machine_status[machine_id].utilization + calculateTaskUtilization(task_id, machine_id);
	if (new_utilization > 1.0) return false;

	return true;
}

// Assumes task isn't on any other machine + needs a vm to run it
bool addTaskToMachine(TaskId_t task_id, MachineId_t machine_id, bool activeMachine) {
	// 1. Find whether machine can support task
	if (!addTaskToMachineChecks(task_id, machine_id)) return false;

	// TODO: Add functionality to ensure we get tasks that need GPUs to GPU machines

	if (!activeMachine) Machine_SetState(machine_id, S0); // Power on the machine if it's inactive

	CPUType_t required_cpu = RequiredCPUType(task_id);
	VMType_t required_vm = RequiredVMType(task_id);
	double new_utilization = machine_status[machine_id].utilization + calculateTaskUtilization(task_id, machine_id);

	// Set priority based on SLA
	Priority_t priority;
	switch(RequiredSLA(task_id)) {
		case SLA0: priority = HIGH_PRIORITY; break;
		case SLA1: priority = MID_PRIORITY; break;
		default:   priority = LOW_PRIORITY;
	}

	// Now, find what, if any, VM can run this task
	bool vm_found = false;
	VMId_t vm_to_use;
	if (activeMachine) {
		// Check if the machine has an active VM that is compatible
		for (VMId_t vm_id : machine_status[machine_id].vms) {
			VMInfo_t vm_info = VM_GetInfo(vm_id);
			if (vm_info.vm_type == required_vm && vm_info.cpu == required_cpu) {
				vm_to_use = vm_id;
				vm_found = true;
				break;
			}
		}
	}
	if (!vm_found) {
		// If we didn't find a compatible VM, create a new one
		vm_to_use = VM_Create(required_vm, required_cpu);
		machine_status[machine_id].vms.push_back(vm_to_use);
		VM_Attach(vm_to_use, machine_id);
	}
	// Now, add task to VM and VM to machine
	VM_AddTask(vm_to_use, task_id, priority);
	machine_status[machine_id].vm_tasks[vm_to_use].push_back(task_id);

	// Update machine status accordingly
	machine_status[machine_id].utilization = new_utilization;
	machine_status[machine_id].tasks.push_back(task_id);
	task_locations[task_id] = {vm_to_use, machine_id};
	if (!activeMachine) inactive_machines_set.erase(machine_id);
	active_machines_set.insert(machine_id);

	return true;
}

bool removeTaskOverheadFromMachine(TaskId_t task_id, bool manuallyRemoveTask = false) {
	// Update the overhead for the task
	auto it = task_locations.find(task_id);
	if (it != task_locations.end()) {
		VMId_t vm_id = it->second.first;
		MachineId_t machine_id = it->second.second;

		// Remove the task location entry
		task_locations.erase(it);
		
		// Update machine status
		machine_status[machine_id].tasks.erase(std::remove(machine_status[machine_id].tasks.begin(), 
														   machine_status[machine_id].tasks.end(), 
														   task_id), 
												machine_status[machine_id].tasks.end());
		
		// Update machine utilization
		machine_status[machine_id].utilization -= calculateTaskUtilization(task_id, machine_id);
		// Ensure utilization does not go negative
		if (machine_status[machine_id].utilization < 0.0) {
			machine_status[machine_id].utilization = 0.0; // Prevent negative utilization
		}

		// Update VM status
		if (manuallyRemoveTask) VM_RemoveTask(vm_id, task_id);
		auto& vm_tasks = machine_status[machine_id].vm_tasks[vm_id];
		vm_tasks.erase(std::remove(vm_tasks.begin(), vm_tasks.end(), task_id), vm_tasks.end());

		// Remove the VM if it has no tasks left
		if (machine_status[machine_id].vm_tasks[vm_id].empty()) {
			VM_Shutdown(vm_id);
			machine_status[machine_id].vms.erase(std::remove(machine_status[machine_id].vms.begin(), 
															machine_status[machine_id].vms.end(), 
															vm_id), 
												machine_status[machine_id].vms.end());
		}
		
		// Power down if the machine has no tasks left
		if (!powerDownActiveMachine(machine_id)) {
			active_machines_set.insert(machine_id); // Ensure machine is marked as active
		}
		return true;
	}
	else {
		SimOutput("WARNING: Task " + to_string(task_id) + " not found in task locations", 0);
		return false;
	}
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
	// Get task parameters
	while (migrating) {
		SimOutput("WARNING: Migration in progress, task " + to_string(task_id) + " will be queued", 0);
		// Don't schedule new tasks during migration
	}

	// Try to find an active machine first (for faster allocation)
	for (MachineId_t machine_id : active_machines_set) {
		// SimOutput("Checking active machine " + to_string(machine_id) + " for task " + to_string(task_id), 0);
		if (addTaskToMachine(task_id, machine_id, true)) return;
	}

	// If no active machine is found, consider turning on a new machine
	for (MachineId_t machine_id : inactive_machines_set) {
		// SimOutput("Checking inactive machine " + to_string(machine_id) + " for task " + to_string(task_id), 0);
		if (addTaskToMachine(task_id, machine_id, false)) return;
	}

	// If we reach here, no suitable machine was found for the task
	SimOutput("WARNING: Could not allocate task " + to_string(task_id), 0);
}

void Scheduler::PeriodicCheck(Time_t now) {
    for(MachineId_t machine_id : active_machines_set) {
        // MachineStatus& status = machine_status[machine_id];
        
        // Power management
        if (powerDownActiveMachine(machine_id)) {
			continue; // Skip to next machine if powered down
		}
		
		for (TaskId_t task_id : machine_status[machine_id].tasks) {
			if (IsSLAViolation(task_id)) {
				SimOutput("WARNING: SLA violation detected for task " + to_string(task_id), 0);
				SLAWarning(now, task_id);
			}
		}
    }
	SimOutput("Scheduler::PeriodicCheck(): Periodic check at time " + to_string(now), 3);
}

void Scheduler::Shutdown(Time_t time) {
    // Do your final reporting and bookkeeping here.
    // Report about the total energy consumed
    // Report about the SLA compliance
    // Shutdown everything to be tidy :-)
    for(auto & vm: vms) {
        VM_Shutdown(vm);
    }

	// Power down all machines
	for(MachineId_t machine_id : machines) {
		Machine_SetState(machine_id, S5);
	}

	// Report total energy consumed
    SimOutput("SimulationComplete(): Finished!", 4);
    SimOutput("SimulationComplete(): Time is " + to_string(time), 4);
}

bool migrateTaskToNewMachine(TaskId_t task_id, MachineId_t source_machine, bool goForHigherUtilization) {
	// Go through active machines and find one with higher utilization
		// Since active machines is set sorted by utilization, just stop once we get to source_machine/find a better machine to transfer this task to
	for (MachineId_t machine_id : active_machines_set) {
		if (machine_id == source_machine) {
			if (goForHigherUtilization) break; // Skip the source machine if we're not looking for higher utilization
			else continue; // Skip the source machine
		}

		if (addTaskToMachineChecks(task_id, machine_id)) {
			removeTaskOverheadFromMachine(task_id, true); // Remove task from source machine
			addTaskToMachine(task_id, machine_id, true); // Add task to target machine
			return true;
		}
		// If we reach here, the machine cannot handle the task, so continue to the next one
	}
	return false;
}

// Add this function after TaskComplete to consolidate machines
void consolidateMachines() {
    // Skip if migration is already in progress
    if (migrating) return;
    
    // Create a vector of active machines sorted by utilization (ascending)
    vector<MachineId_t> sortedMachines(active_machines_set.begin(), active_machines_set.end());
    sort(sortedMachines.begin(), sortedMachines.end(), 
        [](const MachineId_t& a, const MachineId_t& b) {
            return machine_status[a].utilization < machine_status[b].utilization;
        });
    
    // For each low utilization machine, try to migrate tasks to higher utilization machines
    for (size_t i = 0; i < sortedMachines.size(); i++) {
        MachineId_t source_machine = sortedMachines[i];
        
        // Skip if machine is already empty
		MachineInfo_t info = Machine_GetInfo(source_machine);
        if (info.s_state == S5 || machine_status[source_machine].tasks.empty()) {
			SimOutput("Somehow have an empty/inactive machine in active machines set", 0);
            continue;
        }
        
        // Try to migrate as many tasks as possible from this machine
        vector<TaskId_t> tasks_to_migrate = machine_status[source_machine].tasks;
        
        for (TaskId_t task_id : tasks_to_migrate) {
            migrateTaskToNewMachine(task_id, source_machine, true);
        }
        
        powerDownActiveMachine(source_machine); // Power down the machine if it's empty
    }
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    // Do any bookkeeping necessary for the data structures
    // Decide if a machine is to be turned off, slowed down, or VMs to be migrated according to your policy
    // This is an opportunity to make any adjustments to optimize performance/energy
    
	// SimOutput("Scheduler::TaskComplete(): Task " + to_string(task_id) + " completed at time " + to_string(now), 0);

	// Assumption: Task is not attached to VM anymore

	removeTaskOverheadFromMachine(task_id); // Takes care of all necessary updates to overhead

	static int completed_tasks = 0;
    completed_tasks++;
    if (completed_tasks % 10 == 0) {
        // consolidateMachines();
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

	// 1. Find the task with the greatest memory overhead
	unsigned max_memory = 0;
	TaskId_t max_task = 0;
	for (TaskId_t task_id : machine_status[machine_id].tasks) {
		unsigned task_memory = GetTaskMemory(task_id);
		if (task_memory > max_memory) {
			max_memory = task_memory;
			max_task = task_id;
		}
	}
	if (max_task == 0) {
		SimOutput("WARNING: No tasks found on machine " + to_string(machine_id), 0);
		return;
	}

	// 2. Migrate the task to another machine
	if (!migrateTaskToNewMachine(max_task, machine_id, false)) {
		SimOutput("WARNING: Could not migrate task " + to_string(max_task) + " from machine " + to_string(machine_id), 0);
	}
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    // The function is called on to alert you that migration is complete
    SimOutput("MigrationDone(): Migration of VM " + to_string(vm_id) + " was completed at time " + to_string(time), 4);
    Scheduler.MigrationComplete(time, vm_id);
    migrating = false;
}

void SchedulerCheck(Time_t time) {
    // This function is called periodically by the simulator, no specific event
    SimOutput("SchedulerCheck(): SchedulerCheck() called at " + to_string(time), 4);
    Scheduler.PeriodicCheck(time);
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


	// Migrate this task into another machine

	// 1. Find a suitable machine
	// 2. Migrate the task to that machine
	// 3. Update the task location in task_locations map
	// 4. Update the machine status for both the source and destination machines
	// 5. Update the VM status if necessary

	// VM_RemoveTask(task_locations[task_id].first, task_id); // Remove task from VM
	// removeTaskOverheadFromMachine(task_id, true); // Remove task from current machine – definetely don't want

	vector<MachineId_t> sortedMachines;
	for (MachineId_t machine_id : active_machines_set) sortedMachines.push_back(machine_id);
	sort(sortedMachines.begin(), sortedMachines.end(), 
		[](const MachineId_t& a, const MachineId_t& b) {
			return machine_status[a].utilization < machine_status[b].utilization;
		});
	
	// For each low utilization machine, try to migrate tasks to higher utilization machines
	for (size_t i = 0; i < sortedMachines.size(); i++) {
		MachineId_t source_machine = sortedMachines[i];
		
		// Skip if machine is already empty
		if (machine_status[source_machine].tasks.empty()) {
			SimOutput("Somehow have an empty machine in active machines set", 0);
			continue;
		}

		// Add this task into this machine
		if (addTaskToMachineChecks(task_id, source_machine)) {
			removeTaskOverheadFromMachine(task_id, true); // Remove task from source machine
			addTaskToMachine(task_id, source_machine, true); // Add task to target machine
			return;
		}
	}
	// If the machine is not suitable, check for other machines
	SimOutput("WARNING: No suitable machine found for SLA warning on task " + to_string(task_id), 0);
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    // Called in response to an earlier request to change the state of a machine
}

