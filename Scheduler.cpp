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
			// Tie-breaker by ID for consistent ordering
			return a < b;
		}
	};
	set<MachineId_t, MachineUtilizationComparator> active_machines_set; // For sorted access
	set<MachineId_t, MachineUtilizationComparator> inactive_machines_set; // For sorted access
}

void Scheduler::Init() {
    SimOutput("Scheduler::Init(): Total number of machines is " + to_string(Machine_GetTotal()), 3);
    
    // Initialize all machines and VMs
    for (unsigned i = 0; i < Machine_GetTotal(); i++) {
        MachineInfo_t info = Machine_GetInfo(MachineId_t(i));
        machines.push_back(MachineId_t(i));
        
        // Initialize tracking structures for each machine
        machine_status[MachineId_t(i)] = {0.0, vector<TaskId_t>(), vector<VMId_t>()}; // No tasks or VMs yet

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

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    // Get the task parameters
    //  IsGPUCapable(task_id);
    //  GetMemory(task_id);
    //  RequiredVMType(task_id);
    //  RequiredSLA(task_id);
    //  RequiredCPUType(task_id);
    // Decide to attach the task to an existing VM, 
    //      vm.AddTask(taskid, Priority_T priority); or
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
    // }// Skeleton code, you need to change it according to your algorithm

	// Get task parameters
	while (migrating) {
		SimOutput("WARNING: Migration in progress, task " + to_string(task_id) + " will be queued", 0);
		// Don't schedule new tasks during migration
	}
	// TaskInfo_t task_info = GetTaskInfo(task_id);
	CPUType_t required_cpu = RequiredCPUType(task_id);
	VMType_t required_vm = RequiredVMType(task_id);
	// bool needs_gpu = IsTaskGPUCapable(task_id); //TODO: Check GPU capability
	unsigned memory = GetTaskMemory(task_id);
	
	// Calculate task load factor with a more conservative estimate
	// double task_load = double(task_info.total_instructions) / 10000000.0;
	
	// Set priority based on SLA
	Priority_t priority;
	switch(RequiredSLA(task_id)) {
		case SLA0: priority = HIGH_PRIORITY; break;
		case SLA1: priority = MID_PRIORITY; break;
		default:   priority = LOW_PRIORITY;
	}

	// Try to find an active machine first (for faster allocation)
	// for(MachineId_t machine_id : active_machines) {
	for (MachineId_t machine_id : active_machines_set) {
		// SimOutput("Checking active machine " + to_string(machine_id) + " for task " + to_string(task_id), 0);
		MachineInfo_t info = Machine_GetInfo(machine_id);
		
		// Skip machines that don't match requirements
		if(info.cpu != required_cpu) continue;
		// if(needs_gpu && !info.gpus) continue;
		if(info.memory_used + memory > info.memory_size) continue;
		// if(machine_status[machine_id].utilization >= 1.0) continue; // Skip overloaded machines
		// SimOutput("Machine " + to_string(machine_id) + " is not overloaded", 0);

		double new_utilization = machine_status[machine_id].utilization + calculateTaskUtilization(task_id, machine_id);

		if (new_utilization > 1.0) {
			// // If the new utilization exceeds 1.0, skip this machine
			// SimOutput("WARNING: Machine " + to_string(machine_id) + " would exceed capacity with task " + to_string(task_id) + " at utilization of " + to_string(new_utilization), 0);
			continue;
		}

		// SimOutput("Scheduling task " + to_string(task_id) + " on machine " + to_string(machine_id) + " with utilization " + to_string(new_utilization), 0);

		// First, check if the machine has an active VM that is compatible
		VMId_t vm_to_use = VMId_t(-1);
		bool vm_found = false;
		for (VMId_t vm_id : machine_status[machine_id].vms) {
			VMInfo_t vm_info = VM_GetInfo(vm_id);
			if (vm_info.vm_type == required_vm && vm_info.cpu == required_cpu) {
				// Found a compatible VM, add task to it
				// SimOutput("Found compatible VM " + to_string(vm_id) + " on machine " + to_string(machine_id) + " for task " + to_string(task_id), 0);
				vm_to_use = vm_id;
				vm_found = true;
				break;
			}
		}
		// If we didn't find a compatible VM, create a new one
		if (!vm_found) {
			vm_to_use = VM_Create(required_vm, required_cpu);
			VM_Attach(vm_to_use, machine_id);
			machine_status[machine_id].vms.push_back(vm_to_use); // Track the VM used
		}
		// Now we can safely add the task to the VM

		// Add the task to the VM
		VM_AddTask(vm_to_use, task_id, priority);
		// Update machine status
		machine_status[machine_id].utilization = new_utilization;
		machine_status[machine_id].tasks.push_back(task_id);
		task_locations[task_id] = {vm_to_use, machine_id}; // Track task location
		active_machines_set.insert(machine_id); // Resort based on new utilization
		return; // Task successfully scheduled
	}

	// If no active machine is found, consider turning on a new machine
	// for (MachineId_t machine_id : inactive_machines) {
	for (MachineId_t machine_id : inactive_machines_set) {
		MachineInfo_t info = Machine_GetInfo(machine_id);
		
		// Check if the machine can support the task
		if(info.cpu != required_cpu) continue;
		if(info.memory_used + memory > info.memory_size) continue;

		// SimOutput("Powering on inactive machine " + to_string(machine_id) + " for task " + to_string(task_id), 0);
		
		// Power on the machine and set its state to active
		Machine_SetState(machine_id, S0);
		
		// Create a new VM and add the task to it
		VMId_t vm_id = VM_Create(required_vm, required_cpu);
		VM_Attach(vm_id, machine_id);
		VM_AddTask(vm_id, task_id, priority);
		
		machine_status[machine_id].vms.push_back(vm_id); // Track the VM used
		machine_status[machine_id].utilization = calculateTaskUtilization(task_id, machine_id); // Set initial utilization
		machine_status[machine_id].tasks.push_back(task_id); // Track task on this machine
		task_locations[task_id] = {vm_id, machine_id}; // Track task location

		inactive_machines_set.erase(machine_id); // Remove from inactive machines
		active_machines_set.insert(machine_id); // Mark as active

		return; // Task successfully scheduled
	}

	// If we reach here, no suitable machine was found for the task
	SimOutput("WARNING: Could not allocate task " + to_string(task_id), 0);
}

// TODO: Check for SLA Violations here
void Scheduler::PeriodicCheck(Time_t now) {
    // for(MachineId_t machine_id : active_machines_set) {
    //     MachineStatus& status = machine_status[machine_id];
        
    //     // Power management
    //     if(status.tasks.empty()) {
	// 		SimOutput("Scheduler::PeriodicCheck(): No tasks on machine " + to_string(machine_id) + ", powering down", 0);
    //         Machine_SetState(machine_id, S5); // Power down if no tasks are running
	// 		active_machines_set.erase(machine_id); // Remove from active machines
	// 		inactive_machines_set.insert(machine_id); // Move to inactive machines
    //         continue;
    //     }
		
	// 	// TODO: Check for SLA violations and adjust performance if needed
    // }
	SimOutput("Scheduler::PeriodicCheck(): Periodic check at time " + to_string(now), 0);
	return;
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

// TODO: On workload completion, migrate other VMs into this machine to maximize utilization
void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    // Do any bookkeeping necessary for the data structures
    // Decide if a machine is to be turned off, slowed down, or VMs to be migrated according to your policy
    // This is an opportunity to make any adjustments to optimize performance/energy
    
	// SimOutput("Scheduler::TaskComplete(): Task " + to_string(task_id) + " completed at time " + to_string(now), 0);

	// Update the overhead for the task
	auto it = task_locations.find(task_id);
	if (it != task_locations.end()) {
		VMId_t vm_id = it->second.first;
		MachineId_t machine_id = it->second.second;
		
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

		// Remove the VM if it has no tasks left
		bool vm_empty = true;
		for (TaskId_t t : machine_status[machine_id].tasks) {
			if (task_locations[t].first == vm_id) {
				vm_empty = false; // VM still has tasks
				break;
			}
		}
		if (vm_empty) {
			VM_Shutdown(vm_id); // Shutdown the VM if no tasks are left
			auto vm_it = std::find(machine_status[machine_id].vms.begin(), 
									machine_status[machine_id].vms.end(), 
									vm_id);
			if (vm_it != machine_status[machine_id].vms.end()) {
				machine_status[machine_id].vms.erase(vm_it); // Remove VM from tracking
			}
		}
		
		// Power down if the machine has no tasks left
		if (machine_status[machine_id].tasks.empty()) {
			Machine_SetState(machine_id, S5); // Power down if no tasks are running
			active_machines_set.erase(machine_id); // Remove from active machines
			inactive_machines_set.insert(machine_id); // Move to inactive machines
		}

		// Remove the task location entry
		task_locations.erase(it);
	}
	else {
		SimOutput("WARNING: Task " + to_string(task_id) + " not found in task locations", 0);
	}

	// TODO: Refactor all tasks to be migrated to as few machines as possible
		// By pseudocode:
		// Sort machines by utilization (ascending, not descending)
		// For lower utilization, see if tasks can be migrated to higher utilization machines
		// If so, migrate the task to that machine
		// Update the task location in task_locations map
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

	// TODO: Migrate the task to another machine
	// VM_Migrate(VMId_t vm_id, MachineId_t machine_id);
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
    // TODO: Either bump up the performance of the current machine or migrate the task to another machine that is faster
	// Machine_SetCorePerformance(MachineId_t machine_id, unsigned core_id, CPUPerformance_t p_state);


	// Migrate this task into another machine

	// 1. Find a suitable machine
	// 2. Migrate the task to that machine
	// 3. Update the task location in task_locations map
	// 4. Update the machine status for both the source and destination machines
	// 5. Update the VM status if necessary

	for (MachineId_t machine : active_machines_set) {
		// Skip if task is already on this machine
		if (task_locations[task_id].second == machine)
			continue;

		// Check if the machine can handle the task
		MachineInfo_t info = Machine_GetInfo(machine);
		if (info.cpu == RequiredCPUType(task_id) && info.memory_used + GetTaskMemory(task_id) <= info.memory_size) {
			// Check if the machine can handle the task
			VMId_t vm_id = task_locations[task_id].first;
			if (vm_id != VMId_t(-1)) {
				// Migrate the task to this machine
				VM_Migrate(vm_id, machine);
				
				// Update original machine status – both machine status and task_locations
				MachineId_t original_machine = task_locations[task_id].second;
				machine_status[original_machine].tasks.erase(std::remove(machine_status[original_machine].tasks.begin(), 
																		machine_status[original_machine].tasks.end(), 
																		task_id), 
																machine_status[original_machine].tasks.end());
				TaskInfo_t task_info = GetTaskInfo(task_id);
				double task_load = double(task_info.total_instructions) / 10000000.0;
				double mips_factor = double(Machine_GetInfo(original_machine).performance[Machine_GetInfo(original_machine).p_state]) / 3000.0;
				double effective_load = task_load / mips_factor;
				machine_status[original_machine].utilization -= effective_load;
				if (machine_status[original_machine].utilization < 0.0) {
					machine_status[original_machine].utilization = 0.0; // Prevent negative utilization
				}
				// Add back into set
				if (machine_status[original_machine].tasks.empty()) {
					Machine_SetState(original_machine, S5); // Power down if no tasks are running
					active_machines_set.erase(original_machine); // Remove from active machines
					inactive_machines_set.insert(original_machine); // Move to inactive machines
				}
				else
					active_machines_set.insert(original_machine); // Ensure machine is marked as active

				// Update task location
				task_locations[task_id] = {vm_id, machine}; // Update task location to new machine
				machine_status[machine].tasks.push_back(task_id); // Track task on new machine
				machine_status[machine].utilization += effective_load; // Update new machine utilization
				active_machines_set.insert(machine); // Ensure new machine is marked as active
				return; // Task successfully migrated
			}
		}
	}
	// If the machine is not suitable, check for other machines
	SimOutput("WARNING: No suitable machine found for SLA warning on task " + to_string(task_id), 0);
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    // Called in response to an earlier request to change the state of a machine
}

