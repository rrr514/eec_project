//
//  Scheduler.hpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//

#ifndef Scheduler_hpp
#define Scheduler_hpp

#include <vector>

#include "Interfaces.h"

class Scheduler {
public:
    Scheduler()                 {}
    void Init();
    void MigrationComplete(Time_t time, VMId_t vm_id);
    void NewTask(Time_t now, TaskId_t task_id);
    void PeriodicCheck(Time_t now);
    void Shutdown(Time_t now);
    void TaskComplete(Time_t now, TaskId_t task_id);
private:
    vector<VMId_t> vms;
    vector<MachineId_t> machines;
};

struct MachineStatus {
    MachineId_t id;
    vector<VMId_t> vms;
};

bool canRunTask(VMId_t vm, TaskId_t task_id);
bool canAttachVM(MachineStatus* machine);
void migrateVMsToHigherEfficiencyMachines(CPUType_t cpuType);
Time_t computeTaskRemainingRunTime(TaskId_t task_id, MachineId_t machine_id);
Time_t computeVMRemainingRunTime(VMId_t vm_id);
bool isMigratableVM(VMId_t vm_id);
MachineState_t get_machine_s_state(MachineId_t machine_id);


#endif /* Scheduler_hpp */
