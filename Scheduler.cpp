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

// Implementation-specific types and data structures
namespace {
    struct MachineStatus {
        double utilization;
		bool changing_state;
		bool tasks_being_migrated_to;
        vector<TaskId_t> tasks;
		vector<VMId_t> vms; // Track VMs on this machine
    };

    // Global state for scheduler implementation
    map<MachineId_t, MachineStatus> machine_status;
    map<TaskId_t, pair<VMId_t, MachineId_t>> task_locations;
	map<VMId_t, MachineId_t> vm_locations;

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

	set<TaskId_t> tasks_to_do;

	typedef enum {
		GO_HIGHER,
		GO_LOWER,
		DO_NOT_CARE
	} UtilizationPriority;
}

// To ask at office hours:
	// Why CPU issue could be occuring
	// How exactly state changes are logged, since my state changes are not updating somehow

// Problems from Office Hours:
// Check powering down machines and migration separately (do not use together)

void Scheduler::Init() {
    SimOutput("Scheduler::Init(): Total number of machines is " + to_string(Machine_GetTotal()), 3);
    
    // Initialize all machines and VMs
    for (unsigned i = 0; i < Machine_GetTotal(); i++) {
        MachineInfo_t info = Machine_GetInfo(MachineId_t(i));
        machines.push_back(MachineId_t(i));
        
        // Initialize tracking structures for each machine
        machine_status[MachineId_t(i)] = {0.0, true, false, vector<TaskId_t>(), vector<VMId_t>()};

		// All machines are powered on initially
		active_machines_set.insert(MachineId_t(i));
    }
}

// When VM is finished moving into new machine
void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
	machine_status[vm_locations[vm_id]].tasks_being_migrated_to = false;
	SimOutput("Scheduler::MigrationComplete(): Migration of VM " + to_string(vm_id) + " completed at time " + to_string(time), 0);
}

// Entirely created by ChatGPT (Also very unnecessary but too central for my program to remove)
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
	// return false;

	// Power down the machine if it has no tasks
		//  && !machine_status[machine_id].changing_state is not working!
	if (Machine_GetInfo(machine_id).active_tasks == 0 && !machine_status[machine_id].tasks_being_migrated_to
		&& !machine_status[machine_id].changing_state) {
		SimOutput("Scheduler::powerDownActiveMachine(): Powering down machine " + to_string(machine_id), 0);
		Machine_SetState(machine_id, S5); // Power down
		machine_status[machine_id].utilization = 0.0; // Reset utilization
		machine_status[machine_id].tasks.clear();
		machine_status[machine_id].vms.clear();
		active_machines_set.erase(machine_id); // Remove from active machines
		inactive_machines_set.insert(machine_id); // Move to inactive machines
		return true;
	}
	return false;
}

// Assumes task isn't on any other machine + needs a vm to run it
bool addTaskToMachine(TaskId_t task_id, MachineId_t machine_id, bool activeMachine) {
	// if (machine_status[machine_id].changing_state) return false; // Machine is currently changing state
	// if (machine_status[machine_id].tasks_being_migrated_to) return false; // Machine is currently being migrated to

	// Find whether machine can support task
	MachineInfo_t info = Machine_GetInfo(machine_id);
	CPUType_t required_cpu = RequiredCPUType(task_id);
	VMType_t required_vm = RequiredVMType(task_id);
	unsigned memory = GetTaskMemory(task_id);

	if(info.cpu != required_cpu) return false;
	if(info.memory_used + memory > info.memory_size) return false;
	double new_utilization = machine_status[machine_id].utilization + calculateTaskUtilization(task_id, machine_id);
	if (new_utilization > 1.0) return false;

	// TODO: Add functionality to ensure we get tasks that need GPUs to GPU machines

	if (!activeMachine) {
		SimOutput("Waking up machine " + to_string(machine_id) + " for task " + to_string(task_id), 0);
		Machine_SetState(machine_id, S0); // Power on the machine if it's inactive
		machine_status[machine_id].changing_state = true;
		inactive_machines_set.erase(machine_id);
	}
	else active_machines_set.erase(machine_id);

	while (Machine_GetInfo(machine_id).s_state != S0) {
		// Wait for machine to power on
		SimOutput("Waiting for machine " + to_string(machine_id) + " to power on", 0);
	}

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
		vm_locations[vm_to_use] = machine_id;
		machine_status[machine_id].vms.push_back(vm_to_use);
		VM_Attach(vm_to_use, machine_id);
	}
	// Now, add task to VM and VM to machine
	VM_AddTask(vm_to_use, task_id, priority);
	// machine_status[machine_id].vm_tasks[vm_to_use].push_back(task_id);

	// Update machine status accordingly
	machine_status[machine_id].utilization = new_utilization;
	machine_status[machine_id].tasks.push_back(task_id);
	task_locations[task_id] = {vm_to_use, machine_id};
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
		machine_status[machine_id].utilization -= calculateTaskUtilization(task_id, machine_id);
		// Ensure utilization does not go negative
		if (machine_status[machine_id].utilization < 0.0) {
			machine_status[machine_id].utilization = 0.0; // Prevent negative utilization
		}

		// Update VM status
		if (manuallyRemoveTask) VM_RemoveTask(vm_id, task_id);

		// Remove the VM if it has no tasks left
		if (VM_GetInfo(vm_id).active_tasks.empty()) {
			VM_Shutdown(vm_id);
			vm_locations.erase(vm_id);
			machine_status[machine_id].vms.erase(std::remove(machine_status[machine_id].vms.begin(), 
															machine_status[machine_id].vms.end(), 
															vm_id), 
												machine_status[machine_id].vms.end());
		}
		
		// Power down if the machine has no tasks left
		if (!powerDownActiveMachine(machine_id)) {
			active_machines_set.erase(machine_id);
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
	// Try to find an active machine first (for faster allocation)
	for (MachineId_t machine_id : active_machines_set) {
		// SimOutput("Checking active machine " + to_string(machine_id) + " for task " + to_string(task_id), 0);
		if (addTaskToMachine(task_id, machine_id, true)) return;
	}

	// If no active machine is found, consider turning on a new machine
	for (MachineId_t machine_id : inactive_machines_set) {
		// SimOutput("Checking inactive machine " + to_string(machine_id) + " for task " + to_string(task_id), 0);
		if (machine_status[machine_id].changing_state) continue; // Skip machines that are currently changing state -> If in inactive, changing to inactive obviously
		if (addTaskToMachine(task_id, machine_id, false)) return;
	}

	// If we reach here, no suitable machine was found for the task
	// SimOutput("WARNING: Could not allocate task " + to_string(task_id), 0);
	tasks_to_do.insert(task_id);
}

void Scheduler::PeriodicCheck(Time_t now) {
	unsigned num = tasks_to_do.size();
	for (unsigned i = 0; i < num; i++) {
		TaskId_t task_id = *tasks_to_do.begin();
		tasks_to_do.erase(tasks_to_do.begin());
		NewTask(now, task_id);
	}

	// SimOutput("Scheduler::PeriodicCheck(): Periodic check at time " + to_string(now), 0);
	for (MachineId_t machine_id : active_machines_set) {
		powerDownActiveMachine(machine_id);
	}
	// SimOutput("Scheduler::PeriodicCheck(): Periodic check at time " + to_string(now), 0);
}

void Scheduler::Shutdown(Time_t time) {
    // Do your final reporting and bookkeeping here.
    // Report about the total energy consumed
    // Report about the SLA compliance
    // Shutdown everything to be tidy :-)
    for(auto & vm: vms) {
        VM_Shutdown(vm);
		vm_locations.erase(vm);
    }

	// Power down all machines
	for(MachineId_t machine_id : machines) {
		Machine_SetState(machine_id, S5);
	}

	// Report total energy consumed
    SimOutput("SimulationComplete(): Finished!", 4);
    SimOutput("SimulationComplete(): Time is " + to_string(time), 4);
}

bool VM_Check_Machine_Compatibility(VMId_t vm_id, MachineId_t machine_id) {
	// Check if the VM can be attached to the machine
	VMInfo_t vm_info = VM_GetInfo(vm_id);
	MachineInfo_t machine_info = Machine_GetInfo(machine_id);
	if (vm_info.cpu != machine_info.cpu) return false;
	if (machine_info.s_state != S0) return false; // Machine must be powered on
	unsigned vm_memory = 0;
	double vm_utilization = 0.0;
	// SimOutput("Number of active tasks on VM " + to_string(vm_id) + ": " + to_string(vm_info.active_tasks.size()), 0);
	for (TaskId_t task_id : vm_info.active_tasks) {
		vm_memory += GetTaskMemory(task_id);
		vm_utilization += calculateTaskUtilization(task_id, machine_id);
	}
	if (vm_memory + 8 > machine_info.memory_size - machine_info.memory_used) return false;
	if (vm_utilization + machine_status[machine_id].utilization > 1.0) return false;
	return true;
}

bool migrateVMToNewMachine(VMId_t vm_id, MachineId_t source_machine, UtilizationPriority priority = DO_NOT_CARE) {
	for (MachineId_t target_machine : active_machines_set) {
		if (priority == GO_LOWER && machine_status[target_machine].utilization >= machine_status[source_machine].utilization) continue;
		if (priority == GO_HIGHER && machine_status[target_machine].utilization <= machine_status[source_machine].utilization) continue;
		if (target_machine == source_machine) continue; // Skip the source machine

		VMInfo_t vm_info = VM_GetInfo(vm_id);
		if (VM_Check_Machine_Compatibility(vm_id, target_machine)) { // Check if we have the available resources to migrate the VM
			machine_status[source_machine].vms.erase(std::remove(machine_status[source_machine].vms.begin(), 
																machine_status[source_machine].vms.end(), 
																vm_id), 
													machine_status[source_machine].vms.end());
			machine_status[target_machine].vms.push_back(vm_id);
			for (TaskId_t task_id : vm_info.active_tasks) {
				// Update source
				machine_status[source_machine].tasks.erase(std::remove(machine_status[source_machine].tasks.begin(), 
												   machine_status[source_machine].tasks.end(), 
												   task_id), 
										machine_status[source_machine].tasks.end());
				machine_status[source_machine].utilization -= calculateTaskUtilization(task_id, source_machine);
				if (machine_status[source_machine].utilization < 0.0) machine_status[source_machine].utilization = 0.0;
				// Update target
				machine_status[target_machine].tasks.push_back(task_id);
				machine_status[target_machine].utilization += calculateTaskUtilization(task_id, target_machine);
				task_locations[task_id] = {vm_id, target_machine};
			}
			machine_status[target_machine].tasks_being_migrated_to = true; // Should not turn off machine
			VM_Migrate(vm_id, target_machine);
			SimOutput("Migrating VM " + to_string(vm_id) + " from machine " + to_string(source_machine) + " to machine " + to_string(target_machine), 0);
			return true;
		}
	}
	return false;
}

// Add this function after TaskComplete to consolidate machines
void consolidateMachines() {
    // Create a vector of active machines sorted by utilization (ascending)
    vector<MachineId_t> sortedMachines;
	for (const MachineId_t& machine_id : active_machines_set) {
		sortedMachines.push_back(machine_id);
	}
    sort(sortedMachines.begin(), sortedMachines.end(), 
        [](const MachineId_t& a, const MachineId_t& b) {
            return machine_status[a].utilization < machine_status[b].utilization;
        });
    
    // For each low utilization machine, try to migrate tasks to higher utilization machines
    for (size_t i = 0; i < sortedMachines.size(); i++) {
        MachineId_t source_machine = sortedMachines[i];
		if (machine_status[source_machine].changing_state == true) {
			// SimOutput("Machine " + to_string(source_machine) + " is currently changing state", 0);
			continue;
		}
        
        // Skip if machine is already empty
		MachineInfo_t info = Machine_GetInfo(source_machine);
        if (info.s_state != S0 || machine_status[source_machine].tasks.empty()) {
			// SimOutput("Somehow have an empty/inactive machine in active machines set with id: " + to_string(source_machine), 0);
			// SimOutput("Machine " + to_string(source_machine) + " has " + to_string(Machine_GetInfo(source_machine).active_tasks) + " tasks", 0);
        }
		else {
			// Migrate vms on machine to other machines
			for (VMId_t vm_id : machine_status[source_machine].vms) {
				// Migrate all tasks on this VM to other machines
				migrateVMToNewMachine(vm_id, source_machine, GO_HIGHER);
			}
		}

		powerDownActiveMachine(source_machine); // Power down the machine if it's empty
    }
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    // Do any bookkeeping necessary for the data structures
    // Decide if a machine is to be turned off, slowed down, or VMs to be migrated according to your policy
    // This is an opportunity to make any adjustments to optimize performance/energy

	// Assumption: Task is not attached to VM anymore

	removeTaskOverheadFromMachine(task_id); // Takes care of all necessary updates to overhead

	static int completed_tasks = 0;
    completed_tasks++;
    if (completed_tasks % 100 == 0) {
		// SimOutput("Scheduler::TaskComplete(): Consolidating machines at time " + to_string(now), 0);
        consolidateMachines();
		completed_tasks = 0;
    }
    
	// SimOutput("Scheduler::TaskComplete(): Task " + to_string(task_id) + " completed at time " + to_string(now), 0);
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

	// 1. Find VM with greatest memory usage
	unsigned best_vm_memory = 0;
	VMId_t best_vm_id = -1;
	for (VMId_t vm_id : machine_status[machine_id].vms) {
		VMInfo_t vm_info = VM_GetInfo(vm_id);
		unsigned vm_memory = 0;
		for (TaskId_t task_id : vm_info.active_tasks) {
			vm_memory += GetTaskMemory(task_id);
		}
		if (vm_memory > best_vm_memory) {
			best_vm_memory = vm_memory;
			best_vm_id = vm_id;
		}
	}
	if (best_vm_memory == 0) {
		SimOutput("WARNING: No VMs found on machine " + to_string(machine_id), 0);
		return;
	}

	// 2. Migrate tasks from VM to other machines
	migrateVMToNewMachine(best_vm_id, machine_id);
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

	SimOutput("SLAWarning(): SLA violation detected for task " + to_string(task_id), 0);

	// 1. Get the VM of this task to migrate
	VMId_t vm_id = task_locations[task_id].first;
	MachineId_t machine_id = task_locations[task_id].second;

	// 2. Migrate the VM to another machine
	if (!migrateVMToNewMachine(vm_id, machine_id, GO_LOWER)) {
		SimOutput("WARNING: Could not migrate VM " + to_string(vm_id) + " from machine " + to_string(machine_id), 0);
	}
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    // Called in response to an earlier request to change the state of a machine
	machine_status[machine_id].changing_state = false;
	SimOutput("StateChangeComplete(): Machine " + to_string(machine_id) + " has completed state change at time " + to_string(time) + " to state " + to_string(Machine_GetInfo(machine_id).s_state), 0);
}