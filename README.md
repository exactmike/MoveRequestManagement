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

## Sample Workflow
### Setup your command prompt environment
```
Import-Module OneShell
Initialize-AdminEnvironment #Automatically loads the default Organization and Administrative OneShell profiles and connects to the defined systems with the defined credentials
Import-Module MoveRequestManagement
Get-MCTLSourceData -SourceType SQL -SQLConnection $SQLConnections[0] #Uses an existing SQL database connection defined in OneShell to retrieve data directly from a Migration database and creates the $Global:MCTLSourceData variable
$Global:MCTLSourceData = import-csv MyMigrationList.csv #Or use your own data source such as a CSV file, etc.  
$Wave = '2.00' #Define the current wave with which you are working 
```
### Start synchronization of a wave / batch of mailboxes from source Exchange Organization to Target Exchange Organization
```
New-MRMMoveRequest -SourceData $Global:MCTLSourceData -wave $wave -wavetype Sub -SuspendWhenReadyToComplete $true -ExchangeOrganization OL
```
### Initiate a Delta synchronization of a wave / batch of mailboxes
```
Resume-MRMMoveRequestForDeltaSync -wave $wave -wavetype Sub -ExchangeOrganization OL -SourceData $Global:MCTLSourceData
```
### Investigate/Monitor progress from the command line (pulls move requests and statistics into local variables for manual review/reporting)
```
Get-MRMMoveRequestReport -Wave $wave -WaveType Sub -operation WaveMonitoring -statsoperation All -passthru -ExchangeOrganization OL 
```
### Test whether the Move Requests match the Migration Database - Report on Missing or Unexpected Requests or All problems
```
$WaveProblems = Test-MRMConvergence -wave $wave -wavetype Sub -SourceData $global:MCTLSourceData -ExchangeOrganization OL -Report All #OR Missing OR Unexpected
```
### Generate a One Time Emailed Progress Report
```
Watch-MRMMoveRequest -Operation Synchronization -wavetype Sub -wave $Wave -Recipients 'admin@organization.com','projectmanager@organization.com' -Sender 'admin@organization.com' -ExchangeOrganization OL -SourceData $Global:MCTLSourceData 
```
### Generate automatic and periodic emailed progress reports
```
Watch-MRMMoveRequestContinuously -Wave $wave -WaveType Sub -runperiod 60 -Operation Synchronization -ExchangeOrganization OL -SourceData $Global:MCTLSourceData -Recipients 'admin@organization.com','projectmanager@organization.com' -Sender 'admin@organization.com'
```
### Prepare to complete a migration wave / batch - clears the SuspendWhenReadyToComplete switch which saves some time with resuming the requests later at your cutover point
```
Set-MRMMoveRequestForCompletion -wave $wave -wavetype Sub -LargeItemLimit 10 -BadItemLimit 10 -ExchangeOrganization OL -SourceData $Global:MCTLSourceData
```
### Complete a wave/batch and ignore any divergence from the Migration Database with ByPassConvergenceCheck
```
Start-MRMMoveRequestCompletion -wave $wave -wavetype Sub -MigrationBlockList dontmigratehimafterall@organization.com -ExchangeOrganization OL -SourceData $Global:MCTLSourceData -ByPassConvergenceCheck
```