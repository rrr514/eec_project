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

#define MAX_NUM_TASKS_PER_VM 10
#define MAX_NUM_VMS_PER_MACHINE 10

// Implementation-specific types and data structures
namespace {
	typedef enum {
		ACTIVE,
		STANDBY,
		OFF
	} MachineClassification_t;

	struct MachineGroup {
		set<MachineId_t> active;
		set<MachineId_t> standby;
		set<MachineId_t> off;
		map<MachineId_t, vector<VMId_t>> machine_to_vm;
		map<MachineId_t, MachineClassification_t> classification;
		map<TaskId_t, VMId_t> task_to_vm;
		map<TaskId_t, MachineId_t> task_to_machine;
	};

	// Global map from CPU type to the corresponding MachineGroup.
	static map<CPUType_t, MachineGroup> machineGroups;
	vector<TaskId_t> tasks_to_do;
}

// TODO: Ensure you have a data structure to keep track of migrations

void shiftMachine(MachineId_t machine_id, MachineClassification_t oldState, MachineClassification_t newState) {
	CPUType_t cpuType = Machine_GetInfo(machine_id).cpu;
	if (machineGroups.find(cpuType) == machineGroups.end()) return; // Machine group not found, return early

	MachineGroup & group = machineGroups[cpuType];
	// Remove the machine from its old state group
	switch (oldState) {
		case ACTIVE:
			group.active.erase(machine_id);
			break;
		case STANDBY:
			group.standby.erase(machine_id);
			break;
		case OFF:
			group.off.erase(machine_id);
			break;
	}

	// Add the machine to its new state group
	switch (newState) {
		case ACTIVE:
			group.active.insert(machine_id);
			Machine_SetState(machine_id, S0); // Ensure the machine state is set to ACTIVE
			break;
		case STANDBY:
			group.standby.insert(machine_id);
			Machine_SetState(machine_id, S2); // Ensure the machine state is set to STANDBY
			break;
		case OFF:
			group.off.insert(machine_id);
			Machine_SetState(machine_id, S5); // Ensure the machine state is set to OFF
			break;
	}
	// Update the classification of the machine in the group
	group.classification[machine_id] = newState;
}

void Scheduler::Init() {
    SimOutput("Scheduler::Init(): Total number of machines is " + to_string(Machine_GetTotal()), 3);

	// For each machine, assign it to a group based on its CPU type.
    for (MachineId_t m = 0; m < Machine_GetTotal(); m++) {
        MachineInfo_t info = Machine_GetInfo(m);
        CPUType_t cpu = info.cpu;
        // Create group if needed.
        if (machineGroups.find(cpu) == machineGroups.end())
            machineGroups[cpu] = MachineGroup();
        // Initially, put all machines into the "standby" tier.
        machineGroups[cpu].active.insert(m);
    }

	// For each CPU type group, split machines like so:
		// 1/5 active, 2/5 standby, and 2/5 off
	for (auto & pair : machineGroups) {
		MachineGroup & group = pair.second;

		size_t totalMachines = group.active.size();
		size_t numStandby = (totalMachines * 2) / 5; // 2/5 standby
		size_t numOff = numStandby; // Remaining are off

		// Move machines to their respective groups
		while (numStandby > 0 && !group.active.empty()) {
			MachineId_t machine_id = *group.active.begin();
			shiftMachine(machine_id, ACTIVE, STANDBY);
			numStandby--;
		}
		while (numOff > 0 && !group.active.empty()) {
			MachineId_t machine_id = *group.active.begin();
			shiftMachine(machine_id, ACTIVE, OFF);
			numOff--;
		}
	}
}

// When VM is finished moving into new machine
void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
	SimOutput("Scheduler::MigrationComplete(): Migration of VM " + to_string(vm_id) + " completed at time " + to_string(time), 0);
}

bool addTaskToMachine(TaskId_t task_id, MachineId_t machine_id, MachineGroup &group) {
	// For this machine, need to check if we need to add a new VM or add task to existing VM
	bool vm_found = false;
	VMId_t vm_to_use = 0;
	for (VMId_t vm_id : group.machine_to_vm[machine_id]) {
		if (RequiredVMType(task_id) == VM_GetInfo(vm_id).vm_type && VM_GetInfo(vm_id).active_tasks.size() < MAX_NUM_TASKS_PER_VM) {
			// Found the VM that can take this task
			vm_found = true;
			vm_to_use = vm_id;
		}
	}

	// Now, check if machine can handle the memory overhead
	MachineInfo_t machine_info = Machine_GetInfo(machine_id);
	int availablediff = machine_info.memory_size - machine_info.memory_used;
	availablediff -= GetTaskMemory(task_id);
	if (!vm_found) availablediff -= 8;
	if (availablediff < 0) return false; // Not enough memory on this machine, skip to next machine

	Priority_t priority;
	switch(RequiredSLA(task_id)) {
		case SLA0: priority = HIGH_PRIORITY; break;
		case SLA1: priority = MID_PRIORITY; break;
		default:   priority = LOW_PRIORITY;
	}

	// If we reach here, it means we can add the task to this machine
	if (!vm_found) {
		if (group.machine_to_vm[machine_id].size() >= MAX_NUM_VMS_PER_MACHINE) {
			// Cannot add more VMs to this machine
			// SimOutput("addTaskToMachine(): Cannot add task " + to_string(task_id) + " to machine " + to_string(machine_id) + " because it has reached the max number of VMs.", 0);
			return false; // Cannot add task
		}
		vm_to_use = VM_Create(RequiredVMType(task_id), RequiredCPUType(task_id));
		VM_Attach(vm_to_use, machine_id);
		group.machine_to_vm[machine_id].push_back(vm_to_use);
	}
	VM_AddTask(vm_to_use, task_id, priority);
	group.task_to_vm[task_id] = vm_to_use;
	group.task_to_machine[task_id] = machine_id;

	SimOutput("addTaskToMachine(): Added task " + to_string(task_id) + " to VM " + to_string(vm_to_use) +
		" on machine " + to_string(machine_id) + " with priority " + to_string(priority), 0);
	
	return true;
}

// Return true if the task was successfully added to an active machine
	// False if otherwise
bool addToActiveMachine(TaskId_t task_id, MachineGroup &group) {
	for (MachineId_t machine_id : group.active) {
		if (addTaskToMachine(task_id, machine_id, group)) return true;
	}

	// No active machines available – need to get a machine from standby if possible
	for (MachineId_t machine_id : group.standby) {
		if (addTaskToMachine(task_id, machine_id, group)) {
			// If we successfully added the task to a standby machine, we need to shift it to active
			shiftMachine(machine_id, STANDBY, ACTIVE);
			return false; // Need to ensure we return false here to indicate that we did not add to an active machine – want to turn on an off machine
		}
	}

	// If we get here, should only happen because we're out of machines to put into standby – wait for next check
	tasks_to_do.push_back(task_id);
	SimOutput("addToActiveMachine(): No active machines available for task " + to_string(task_id) + ". Will retry later.", 0);
	return false;
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
	CPUType_t req_cpu = RequiredCPUType(task_id);
	auto it = machineGroups.find(req_cpu);
    if (it == machineGroups.end()) {
        SimOutput("NewTask(): No machines available for CPU type " + to_string(req_cpu), 0);
        return;
    }
    MachineGroup &group = it->second;

	// Add this task to the active machines that are available for scheduling.
	if (addToActiveMachine(task_id, group)) return;

	// Since we're here, try to get one machine in off to standby – because we returned false
	for (MachineId_t machine_id : group.off) {
		shiftMachine(machine_id, OFF, STANDBY); // Change state from OFF to STANDBY
		return;
	}
}

void Scheduler::PeriodicCheck(Time_t now) {
	// SimOutput("Scheduler::PeriodicCheck(): Periodic check at time " + to_string(now), 0);

	for (TaskId_t task_id : tasks_to_do)
		// Try to re-add the task to the active machines
		NewTask(now, task_id);
}

void Scheduler::Shutdown(Time_t time) {
    // Do your final reporting and bookkeeping here.
    // Report about the total energy consumed
    // Report about the SLA compliance
    // Shutdown everything to be tidy :-)
    for (auto & pair : machineGroups) {
		MachineGroup & group = pair.second;
		for (auto & pair: group.machine_to_vm) {
			MachineId_t machine_id = pair.first;
			vector<VMId_t> &vms = pair.second;

			for (VMId_t vm_id : vms) VM_Shutdown(vm_id);
			Machine_SetState(machine_id, S5); // Set the machine to OFF state
		}
	}

	// Report total energy consumed
    SimOutput("SimulationComplete(): Finished!", 4);
    SimOutput("SimulationComplete(): Time is " + to_string(time), 4);
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    // Do any bookkeeping necessary for the data structures
    // Decide if a machine is to be turned off, slowed down, or VMs to be migrated according to your policy
    // This is an opportunity to make any adjustments to optimize performance/energy

	// On Task Complete, need to check what active machine the task is on + if that active machine is done
		// If that active machine has no more tasks – can make standby + make a standby into off
	CPUType_t req_cpu = RequiredCPUType(task_id);
	auto it = machineGroups.find(req_cpu);
	if (it == machineGroups.end()) {
		SimOutput("TaskComplete(): Somehow do not have mapping for " + to_string(req_cpu), 0);
		return;
	}
	MachineGroup &group = it->second;
	MachineId_t machine_id = group.task_to_machine[task_id];
	group.task_to_machine.erase(task_id);
	VMId_t vm_id = group.task_to_vm[task_id];
	group.task_to_vm.erase(task_id);

	// First, see if VM has any more tasks
	if (VM_GetInfo(vm_id).active_tasks.size() == 0) {
		// If there are no more tasks in this VM, we can remove it from the machine
		auto &machine_vms = group.machine_to_vm[machine_id];
		machine_vms.erase(remove(machine_vms.begin(), machine_vms.end(), vm_id), machine_vms.end());

		// Shutdown the VM
		VM_Shutdown(vm_id);

		// Check if the machine can be moved to standby
		if (group.machine_to_vm[machine_id].empty()) {
			if (!group.standby.empty()) { // Move one machine from standby to off
				MachineId_t standby_machine_id = *group.standby.begin();
				shiftMachine(standby_machine_id, STANDBY, OFF);
			}
			shiftMachine(machine_id, ACTIVE, STANDBY); // Move to standby
		}
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
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    // Called in response to an earlier request to change the state of a machine
	// SimOutput("StateChangeComplete(): Machine " + to_string(machine_id) + " has completed state change at time " + to_string(time) + " to state " + to_string(Machine_GetInfo(machine_id).s_state), 0);
}