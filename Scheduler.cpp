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
		bool tasks_being_migrated_to;
        set<TaskId_t> tasks;
		set<VMId_t> vms;
		set<VMId_t> overloaded_vms;
    };

    // Global state for scheduler implementation
    map<MachineId_t, MachineStatus> machine_status;
    map<TaskId_t, pair<VMId_t, MachineId_t>> task_locations;
	map<VMId_t, MachineId_t> vm_locations;
	set<VMId_t> vms_to_migrate;

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
	struct ReverseMachineUtilizationComparator {
		bool operator()(const MachineId_t& a, const MachineId_t& b) const {
			// Compare by utilization
			if (machine_status[a].utilization != machine_status[b].utilization)
				return machine_status[a].utilization < machine_status[b].utilization;

			// Check if either has a gpu while the other doesn't
			if (Machine_GetInfo(a).gpus != Machine_GetInfo(b).gpus)
				return Machine_GetInfo(a).gpus > Machine_GetInfo(b).gpus;

			// Otherwise, sort by memory used by each machine
			if (Machine_GetInfo(a).memory_used != Machine_GetInfo(b).memory_used)
				return Machine_GetInfo(a).memory_used < Machine_GetInfo(b).memory_used;
			
			// Tie-breaker by ID for consistent ordering
			return a < b;
		}
	};
	map<CPUType_t, set<MachineId_t, MachineUtilizationComparator>> active_machines_map;
	map<CPUType_t, set<MachineId_t, MachineUtilizationComparator>> inactive_machines_map;
	map<CPUType_t, unsigned int> count_map;
	map<CPUType_t, bool> machine_is_being_activated;
	map<CPUType_t, set<MachineId_t, ReverseMachineUtilizationComparator>> overloaded_machines_map;

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
		// SimOutput("Scheduler::Init(): Initializing machine " + to_string(i), 0);
        MachineInfo_t info = Machine_GetInfo(MachineId_t(i));
        machines.push_back(MachineId_t(i));
        
        // Initialize tracking structures for each machine
        machine_status[MachineId_t(i)] = {0.0, false, set<TaskId_t>(), set<VMId_t>(), set<VMId_t>()};

		// All machines are powered on initially
		CPUType_t cpu_type = info.cpu;
		if (active_machines_map.find(cpu_type) == active_machines_map.end()) {
			active_machines_map[cpu_type] = set<MachineId_t, MachineUtilizationComparator>();
			inactive_machines_map[cpu_type] = set<MachineId_t, MachineUtilizationComparator>();
			count_map[cpu_type] = 0;
			machine_is_being_activated[cpu_type] = false;
			overloaded_machines_map[cpu_type] = set<MachineId_t, ReverseMachineUtilizationComparator>();
		}
		active_machines_map[cpu_type].insert(MachineId_t(i));
		count_map[cpu_type]++;
    }
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

// When VM is finished moving into new machine
void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
	vms_to_migrate.erase(vm_id);

	if (VM_GetInfo(vm_id).active_tasks.empty()) {
		SimOutput("Scheduler::MigrationComplete(): VM " + to_string(vm_id) + " has no tasks, shutting down", 0);
		VM_Shutdown(vm_id);
		vm_locations.erase(vm_id); // Clean up the mapping
		return;
	}

	MachineId_t target_machine = vm_locations[vm_id];
	// Update Target machine status
	machine_status[target_machine].tasks_being_migrated_to = false;
	machine_status[target_machine].vms.insert(vm_id);
	for (TaskId_t task_id : VM_GetInfo(vm_id).active_tasks) {
		machine_status[target_machine].tasks.insert(task_id);
		machine_status[target_machine].utilization += calculateTaskUtilization(task_id, target_machine);
		task_locations[task_id] = {vm_id, target_machine};
	}
	// SimOutput("Scheduler::MigrationComplete(): Migration of VM " + to_string(vm_id) + " completed at time " + to_string(time), 0);
}

bool powerDownActiveMachine(MachineId_t machine_id) {
	// return false;

	// SimOutput("Scheduler::powerDownActiveMachine(): Checking machine " + to_string(machine_id) + " for power down", 0);

	// Power down the machine if it has no tasks
		//  && !machine_status[machine_id].changing_state is not working!
	MachineInfo_t info = Machine_GetInfo(machine_id);
	if (info.active_tasks == 0 && !machine_status[machine_id].tasks_being_migrated_to
		&& active_machines_map[info.cpu].size() > (count_map[info.cpu] / 5)) {
		// SimOutput("Scheduler::powerDownActiveMachine(): Powering down machine " + to_string(machine_id), 0);
		Machine_SetState(machine_id, S5);
		machine_status[machine_id].utilization = 0.0;
		machine_status[machine_id].tasks.clear();
		machine_status[machine_id].vms.clear();
		active_machines_map[info.cpu].erase(machine_id);
		overloaded_machines_map[info.cpu].erase(machine_id);
		return true;
	}
	return false;
}

// Assumes task isn't on any other machine + needs a vm to run it
bool addTaskToMachine(TaskId_t task_id, MachineId_t machine_id) {
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

	active_machines_map[info.cpu].erase(machine_id);

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
	// Check if the machine has an active VM that is compatible
	for (VMId_t vm_id : machine_status[machine_id].vms) {
		VMInfo_t vm_info = VM_GetInfo(vm_id);
		if (vm_info.vm_type == required_vm && vm_info.cpu == required_cpu) {
			vm_to_use = vm_id;
			vm_found = true;
			break;
		}
	}
	if (!vm_found) {
		// If we didn't find a compatible VM, create a new one
		vm_to_use = VM_Create(required_vm, required_cpu);
		vm_locations[vm_to_use] = machine_id;
		machine_status[machine_id].vms.insert(vm_to_use);
		VM_Attach(vm_to_use, machine_id);
	}
	// Now, add task to VM and VM to machine
	VM_AddTask(vm_to_use, task_id, priority);
	// machine_status[machine_id].vm_tasks[vm_to_use].push_back(task_id);

	// Update machine status accordingly
	machine_status[machine_id].utilization = new_utilization;
	machine_status[machine_id].tasks.insert(task_id);
	task_locations[task_id] = {vm_to_use, machine_id};
	active_machines_map[info.cpu].insert(machine_id);

	return true;
}

bool overloadTaskToMachine(TaskId_t task_id, MachineId_t machine_id, bool currentlyActive) {
	MachineInfo_t info = Machine_GetInfo(machine_id);
	CPUType_t required_cpu = RequiredCPUType(task_id);
	if (info.cpu != required_cpu) return false;
	VMType_t required_vm = RequiredVMType(task_id);
	unsigned memory = GetTaskMemory(task_id);

	double new_utilization = machine_status[machine_id].utilization + calculateTaskUtilization(task_id, machine_id);
	active_machines_map[info.cpu].erase(machine_id);

	Priority_t priority;
	switch(RequiredSLA(task_id)) {
		case SLA0: priority = HIGH_PRIORITY; break;
		case SLA1: priority = MID_PRIORITY; break;
		default:   priority = LOW_PRIORITY;
	}

	if (currentlyActive) active_machines_map[info.cpu].erase(machine_id);
	else overloaded_machines_map[info.cpu].erase(machine_id);

	// Create new VM specifically for this task or find an existing one
	bool vm_found = false;
	VMId_t vm_to_use;
	for (VMId_t vm_id : machine_status[machine_id].overloaded_vms) {
		VMInfo_t vm_info = VM_GetInfo(vm_id);
		if (vm_info.vm_type == required_vm && vm_info.cpu == required_cpu) {
			vm_to_use = vm_id;
			vm_found = true;
			break;
		}
	}
	if (!vm_found) {
		vm_to_use = VM_Create(required_vm, required_cpu);
		vm_locations[vm_to_use] = machine_id;
		machine_status[machine_id].overloaded_vms.insert(vm_to_use);
		VM_Attach(vm_to_use, machine_id);
	}
	VM_AddTask(vm_to_use, task_id, priority);

	machine_status[machine_id].utilization = new_utilization;
	machine_status[machine_id].tasks.insert(task_id);
	task_locations[task_id] = {vm_to_use, machine_id};
	overloaded_machines_map[info.cpu].insert(machine_id);
	return true;
}

bool removeTaskOverheadFromMachine(TaskId_t task_id) {
	// Update the overhead for the task
	auto it = task_locations.find(task_id);
	if (it != task_locations.end()) {
		VMId_t vm_id = it->second.first;
		MachineId_t machine_id = it->second.second;

		// Remove the task location entry
		task_locations.erase(it);
		
		// Update machine status
		machine_status[machine_id].tasks.erase(task_id);
		machine_status[machine_id].utilization -= calculateTaskUtilization(task_id, machine_id);
		// Ensure utilization does not go negative
		if (machine_status[machine_id].utilization < 0.0) {
			machine_status[machine_id].utilization = 0.0; // Prevent negative utilization
		}

		// Remove the VM if it has no tasks left
		if (VM_GetInfo(vm_id).active_tasks.empty() && !vms_to_migrate.count(vm_id)) {
			// SimOutput("Shutting down VM " + to_string(vm_id) + " from machine " + to_string(machine_id), 0);
			VM_Shutdown(vm_id);
			vm_locations.erase(vm_id);
			bool overloaded = machine_status[machine_id].overloaded_vms.count(vm_id);
			if (overloaded) machine_status[machine_id].overloaded_vms.erase(vm_id);
			else machine_status[machine_id].vms.erase(vm_id);

			// Power down if the machine has no tasks left
			if (!powerDownActiveMachine(machine_id)) {
				CPUType_t cpu_type = Machine_GetCPUType(machine_id);
				if (overloaded) overloaded_machines_map[cpu_type].erase(machine_id);
				else active_machines_map[cpu_type].erase(machine_id);
				active_machines_map[cpu_type].insert(machine_id); // Ensure machine is marked as active
			}
		}
		return true;
	}
	// SimOutput("WARNING: Task " + to_string(task_id) + " not found in task locations", 0);
	// Most likely a task on a VM that was being migrated – not an active task anyway
	return false;
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
	// SimOutput("Scheduler::NewTask(): Received new task " + to_string(task_id) + " at time " + to_string(now), 0);

	// Try to find an active machine first (for faster allocation)
	CPUType_t required_cpu = RequiredCPUType(task_id);
	for (MachineId_t machine_id : active_machines_map[required_cpu]) {
		// SimOutput("Checking active machine " + to_string(machine_id) + " for task " + to_string(task_id), 0);
		if (addTaskToMachine(task_id, machine_id)) return;
	}

	// If no active machine is found, consider turning on a new machine
	for (MachineId_t machine_id : inactive_machines_map[required_cpu]) {
		// SimOutput("Checking inactive machine " + to_string(machine_id) + " for task " + to_string(task_id), 0);
		if (machine_is_being_activated[required_cpu]) break; // Skip if already activating
		inactive_machines_map[required_cpu].erase(machine_id);
		Machine_SetState(machine_id, S0);
		machine_is_being_activated[required_cpu] = true;
		break;
	}

	// If we reach here, no suitable machine was found for the task
		// Now have to add to an active machine to make it overloaded
	for (MachineId_t machine_id : overloaded_machines_map[required_cpu]) {
		// SimOutput("Overloading overloaded machine " + to_string(machine_id) + " for task " + to_string(task_id), 0);
		if (overloadTaskToMachine(task_id, machine_id, false)) return;
	}
	for (MachineId_t machine_id : active_machines_map[required_cpu]) {
		// SimOutput("Overloading active machine " + to_string(machine_id) + " for task " + to_string(task_id), 0);
		if (overloadTaskToMachine(task_id, machine_id, true)) return;
	}
	SimOutput("WARNING: Did not schedule task " + to_string(task_id), 0);
}

void Scheduler::PeriodicCheck(Time_t now) {
	// SimOutput("Scheduler::PeriodicCheck(): Periodic check at time " + to_string(now), 0);
	// Make a copy of the active machines set to avoid modifying it while iterating
	for (const auto & machine_pair : active_machines_map) {
		// CPUType_t cpu_type = machine_pair.first;
		const set<MachineId_t, MachineUtilizationComparator>& machines = machine_pair.second;

		// Make a copy of the machines set to avoid modifying it while iterating
		set<MachineId_t, MachineUtilizationComparator> machines_copy = machines;
		for (MachineId_t machine_id : machines_copy) {
			if (machine_status[machine_id].utilization < 0.1) {
				powerDownActiveMachine(machine_id);
			}
		}
	}
	// SimOutput("Scheduler::PeriodicCheck(): Periodic check at time " + to_string(now), 0);
}

void Scheduler::Shutdown(Time_t time) {
    // Do your final reporting and bookkeeping here.
    // Report about the total energy consumed
    // Report about the SLA compliance
    // Shutdown everything to be tidy :-)

	// Shutdown all vms
	for (const auto& pair : vm_locations) {
		VMId_t vm_id = pair.first;
		// MachineId_t machine_id = pair.second;
		// Shutdown the VM
		VM_Shutdown(vm_id);
		vm_locations.erase(vm_id); // Clean up the mapping
	}
    
	// Power down all active machines
	for (const auto& pair : active_machines_map) {
		// CPUType_t cpu_type = pair.first;
		const set<MachineId_t, MachineUtilizationComparator>& machines = pair.second;

		for (MachineId_t machine_id : machines) {
			Machine_SetState(machine_id, S5); // Power down the machine
			machine_status[machine_id].utilization = 0.0; // Reset utilization
			machine_status[machine_id].tasks.clear();
			machine_status[machine_id].vms.clear();
		}
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
	for (MachineId_t target_machine : active_machines_map[VM_GetInfo(vm_id).cpu]) {
		if (priority == GO_LOWER && machine_status[target_machine].utilization >= machine_status[source_machine].utilization) continue;
		if (priority == GO_HIGHER && machine_status[target_machine].utilization <= machine_status[source_machine].utilization) continue;
		if (target_machine == source_machine) continue; // Skip the source machine

		VMInfo_t vm_info = VM_GetInfo(vm_id);
		if (VM_Check_Machine_Compatibility(vm_id, target_machine)) { // Check if we have the available resources to migrate the VM
			machine_status[source_machine].vms.erase(vm_id);
			for (TaskId_t task_id : vm_info.active_tasks) {
				// Update source
				machine_status[source_machine].tasks.erase(task_id);
				machine_status[source_machine].utilization -= calculateTaskUtilization(task_id, source_machine);
				if (machine_status[source_machine].utilization < 0.0) machine_status[source_machine].utilization = 0.0;
				task_locations.erase(task_id); // Remove task location
			}
			machine_status[target_machine].tasks_being_migrated_to = true; // Should not turn off machine
			vm_locations[vm_id] = target_machine; // Update VM location
			vms_to_migrate.insert(vm_id); // Mark VM for migration
			// Will update target overhead once VM is there
			// SimOutput("Migrating VM " + to_string(vm_id) + " from machine " + to_string(source_machine) + " to machine " + to_string(target_machine), 0);
			VM_Migrate(vm_id, target_machine);
			return true;
		}
	}
	return false;
}

void consolidateMachines(CPUType_t cpu_type) {
    // Create a vector of active machines sorted by utilization (ascending)
    vector<MachineId_t> sortedMachines;
	for (const MachineId_t& machine_id : active_machines_map[cpu_type]) {
		sortedMachines.push_back(machine_id);
	}
    sort(sortedMachines.begin(), sortedMachines.end(), 
        [](const MachineId_t& a, const MachineId_t& b) {
            return machine_status[a].utilization < machine_status[b].utilization;
        });
    
    // For each low utilization machine, try to migrate tasks to higher utilization machines
    for (size_t i = 0; i < sortedMachines.size(); i++) {
        MachineId_t source_machine = sortedMachines[i];
		// if (machine_status[source_machine].changing_state == true) {
		// 	// SimOutput("Machine " + to_string(source_machine) + " is currently changing state", 0);
		// 	continue;
		// }
        
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

		if (!powerDownActiveMachine(source_machine)) {
			active_machines_map[cpu_type].erase(source_machine);
			active_machines_map[cpu_type].insert(source_machine);
		}
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
    if (completed_tasks % 1000 == 0) {
		// SimOutput("Scheduler::TaskComplete(): Consolidating machines at time " + to_string(now), 0);
        for (const auto& pair : active_machines_map) {
			CPUType_t cpu_type = pair.first;

			// SimOutput("Scheduler::TaskComplete(): Consolidating machines for CPU type " + to_string(cpu_type) + " at time " + to_string(now), 0);

			// Consolidate machines for this CPU type
			// consolidateMachines(cpu_type);
		}
		completed_tasks = 0;
    }
    
	// SimOutput("Scheduler::TaskComplete(): Task " + to_string(task_id) + " completed at time " + to_string(now), 0);

	// Check if any machine still has a task left to complete
	// for (const auto& pair : active_machines_map) {
	// 	// CPUType_t cpu_type = pair.first;
	// 	const set<MachineId_t, MachineUtilizationComparator>& machines = pair.second;

	// 	for (MachineId_t machine_id : machines) {
	// 		if (!machine_status[machine_id].tasks.empty()) {
	// 			SimOutput("Scheduler::TaskComplete(): Machine " + to_string(machine_id) + " still has tasks left", 0);
	// 		}
	// 	}
	// }
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
    SimOutput("MemoryWarning(): Overflow at " + to_string(machine_id) + " was detected at time " + to_string(time), 4);

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
	// migrateVMToNewMachine(best_vm_id, machine_id);
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

	SimOutput("SLAWarning(): SLA violation detected for task " + to_string(task_id) + " at time " + to_string(time), 4);

	if (task_locations.find(task_id) == task_locations.end()) {
		SimOutput("WARNING: Task " + to_string(task_id) + " not found in task locations", 0);
		// Most likely a task on a VM that was being migrated – meaning we've already solved it anyway
		return;
	}

	// 1. Get the VM of this task to migrate
	VMId_t vm_id = task_locations[task_id].first;
	MachineId_t machine_id = task_locations[task_id].second;

	// 2. Migrate the VM to another machine
	// if (!migrateVMToNewMachine(vm_id, machine_id, GO_LOWER)) {
	// 	SimOutput("WARNING: Could not migrate VM " + to_string(vm_id) + " from machine " + to_string(machine_id), 0);
	// }
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    // Called in response to an earlier request to change the state of a machine
	CPUType_t cpu = Machine_GetCPUType(machine_id);
	if (Machine_GetInfo(machine_id).s_state == S0) {
		active_machines_map[cpu].insert(machine_id); // Add to active machines
		machine_is_being_activated[cpu] = false;

		// Want to migrate VMs from overloaded machines to this machine
		for (MachineId_t source_machine : overloaded_machines_map[cpu]) {
			// SimOutput("Scheduler::StateChangeComplete(): Migrating VMs from overloaded machine " + to_string(machine_id) + " to machine " + to_string(machine_id), 0);
			for (VMId_t vm_id : machine_status[machine_id].overloaded_vms) {
				VMInfo_t vm_info = VM_GetInfo(vm_id);
				if (VM_Check_Machine_Compatibility(vm_id, machine_id)) { // Check if we have the available resources to migrate the VM
					machine_status[source_machine].vms.erase(vm_id);
					for (TaskId_t task_id : vm_info.active_tasks) {
						// Update source
						machine_status[source_machine].tasks.erase(task_id);
						machine_status[source_machine].utilization -= calculateTaskUtilization(task_id, source_machine);
						if (machine_status[source_machine].utilization < 0.0) machine_status[source_machine].utilization = 0.0;
						task_locations.erase(task_id); // Remove task location
					}
					machine_status[machine_id].tasks_being_migrated_to = true;
					vm_locations[vm_id] = machine_id;
					vms_to_migrate.insert(vm_id); 
					// Will update target overhead once VM is there
					// SimOutput("Migrating VM " + to_string(vm_id) + " from machine " + to_string(source_machine) + " to machine " + to_string(target_machine), 0);
					VM_Migrate(vm_id, machine_id);

					// At this point, check if source machine is overloaded any longer – add to active machines at that point
					if (machine_status[source_machine].overloaded_vms.empty()) {
						overloaded_machines_map[cpu].erase(source_machine);
						active_machines_map[cpu].insert(source_machine); // Add back to active machines
					}
				}
				if (machine_status[machine_id].utilization > 0.7) return;
			}
		}
	}
	else if (Machine_GetInfo(machine_id).s_state == S5) {
		inactive_machines_map[cpu].insert(machine_id); // Add to inactive machines
	}
	else {
		SimOutput("WARNING: Machine " + to_string(machine_id) + " is in an unknown state after state change", 0);
	}

	// SimOutput("StateChangeComplete(): State change complete for machine " + to_string(machine_id), 4);
	// SimOutput("StateChangeComplete(): Machine " + to_string(machine_id) + " is now in state " + to_string(Machine_GetInfo(machine_id).s_state), 4);
	// SimOutput("StateChangeComplete(): Machine " + to_string(machine_id) + " has utilization " + to_string(machine_status[machine_id].utilization), 4);
	// SimOutput("StateChangeComplete(): Machine " + to_string(machine_id) + " has tasks: ", 4);
	// for (TaskId_t task : machine_status[machine_id].tasks) {
	//     SimOutput("StateChangeComplete(): Task ID: " + to_string(task), 4);
	// }
	SimOutput("StateChangeComplete(): Machine " + to_string(machine_id) + " has completed state change at time " + to_string(time) + " to state " + to_string(Machine_GetInfo(machine_id).s_state), 4);
}