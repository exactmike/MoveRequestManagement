# MoveRequestManagement
MoveRequestManagement is a companion module to OneShell which contains functions for the management of mailbox migrations between Exchange Organizations.
## Key Features
 - Migration data/database driven operations for coordination with business users/processes
 - Batch creation of mailbox move requests in waves or batches (uses the batchname attribute of move requests NOT migration batches)
 - Batch initiation of delta synchronization for move requests in a wave/batch.
 - Batch initiation of migration completion for move requests in a wave/batch.
 - Validation of batch membership convergence with the defined wave/batch (helps manage changes or exceptions to avoid missing or unwanted mailbox migrations in dynamic environments).
 - Batch operations monitoring via command line or automated recurring email progress and problem reports
 - Large Item reporting (items too large to migrate) including item details and location in mailbox
 - Under development (post migration configuration such as permission verification/re-application, forwarding configurations, quota configuration, etc.)
## Requirements
 - OneShell deployment with at least 2 Exchange Organizations defined in the profile
 - Data requirements for the data to be used with the -SourceData parameter of the MoveRequestManagement functions which require it.  The Source Data must include the following attributes/properties:
   - Wave (the particular batch/wave the mailbox is to be migrated with).  Numeric format in the following style X.XX - XX.XX.
   - SourceSystem (an identifier of the source Exchange system for the given mailbox - must match the value for the Source Exchange Organization from the OneShell organization profile)
   - ExchangeGuid (the source mailbox's ExchangeGuid which also must be synchronized or stamped on to the target mailuser object)
   - UserPrincipalName and PrimarySMTPAddress (used primarily for 'friendly' data about the mailbox in logs/reports but also used as an identifier with the MigrationBlockList feature that some functions in MRM support)
   - Optional: MRSGroup (may be specified in order to assign the mailbox to a particular set of MRS Proxy endpoints - useful for multi-data center organizations where mailbox data should be pulled from a specific endpoint, or for automated load balancing between non-loadbalanced endpoints)
 
   
   