IPC-DirQueue
============

Perl disk-based many-to-many task queue.

This module implements a FIFO queueing infrastructure, using a directory as the communications and storage media.
No daemon process is required to manage the queue; all communication takes place via the filesystem.

A common UNIX system design pattern is to use a tool like lpr as a task queueing system; for example,
http://patrick.wagstrom.net/old/weblog/archives/000128.html describes the use of lpr as an MP3 jukebox.

However, lpr isn't as efficient as it could be. When used in this way, you have to restart each task processor for 
every new task. If you have a lot of startup overhead, this can be very inefficient. With IPC::DirQueue, a processing 
server can run persistently and cache data needed across multiple tasks efficiently; it will not be restarted unless 
you restart it.

Multiple enqueueing and dequeueing processes on multiple hosts (NFS-safe locking is used) can run simultaneously, and 
safely, on the same queue.

Since multiple dequeuers can run simultaneously, this provides a good way to process a variable level of incoming 
tasks using a pre-defined number of worker processes.

If you need more CPU power working on a queue, you can simply start another dequeuer to help out. If you need less, 
kill off a few dequeuers.

If you need to take down the server to perform some maintainance or upgrades, just kill the dequeuer processes, 
perform the work, and start up new ones. Since there's no 'socket' or similar point of failure aside from the directory 
itself, the queue will just quietly fill with waiting jobs until the new dequeuer is ready.

Arbitrary 'name = value' string-pair metadata can be transferred alongside data files. In fact, in some cases, you 
may find it easier to send unused and empty data files, and just use the 'metadata' fields to transfer the details 
of what will be worked on.


http://search.cpan.org/~jmason/IPC-DirQueue-1.0/lib/IPC/DirQueue.pm
