# Forker
A class for running multiple tasks. The total number of concurrently running tasks is limited. 
If no free slot for a task is available, the task is put into the tasks queue. If a free slot 
appears (because a task just finished), the thask is removed from the tasks queue and is executed.
User of the Forker will be informed when all tasks are finished.