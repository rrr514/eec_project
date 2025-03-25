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
    unsigned utilization;
    vector<VMId_t> vms;
};

bool canRunTask(VMId_t vm, TaskId_t task_id);
bool canAttachVM(MachineStatus machine);
double calculateTaskUtilization(TaskId_t task_id, MachineId_t machine_id);


#endif /* Scheduler_hpp */
