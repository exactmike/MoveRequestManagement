###################################################################################################
#Module Variables and Variable Functions
###################################################################################################
function Get-MRMVariable
{
param
(
[string]$Name
)
    Get-Variable -Scope Script -Name $name 
}
function Get-MRMVariableValue
{
param
(
[string]$Name
)
    Get-Variable -Scope Script -Name $name -ValueOnly
}
function Set-MRMVariable
{
param
(
[string]$Name
,
$Value
)
    Set-Variable -Scope Script -Name $Name -Value $value  
}
function New-MRMVariable
{
param 
(
[string]$Name
,
$Value
)
    New-Variable -Scope Script -Name $name -Value $Value
}
function Remove-MRMVariable
{
param
(
[string]$Name
)
    Remove-Variable -Scope Script -Name $name
}
###################################################################################################
#move request functions
###################################################################################################
Function New-MRMMoveRequest
{
[cmdletbinding()]
param
(
$SourceData = $Global:sourcedata
,
$LogFileBaseName = ('_NewWaveBatchMoveRequest.log')
,
[parameter(Mandatory=$True)]
[string]$wave
,
[parameter(Mandatory=$True)]
[ValidateSet('Full','Sub')]
[string]$wavetype
#,
#[datetime]$StartTime #experimental
,
[int]$LargeItemLimit = 50
,
[int]$BadItemLimit = 50
,
[bool]$SuspendWhenReadyToComplete = $True
,
[bool]$Suspend = $False
,
[parameter(Mandatory=$true)]
[string]$ExchangeOrganization #make this a dynamic parameter later
)
    [string]$Stamp = Get-Date -Format yyyyMMdd-HHmm
    $LogPath = $Global:LogFolderPath + $stamp + '-' + $wave + $LogFileBaseName
    $ErrorLogPath = $Global:LogFolderPath + $stamp + '-' + $wave + '-ERRORS' + $LogFileBaseName
    switch ($wavetype) 
    {
        'Full' {$WaveData = @($SourceData | Where-Object {$_.Wave -match "\b$wave(\.\S*|\b)"})} #-and $_.RecipientStatus -notin ("Missing","Duplicate")})}
        'Sub' {$WaveData = @($SourceData | Where-Object {$_.Wave -eq $wave})} #-and $_.RecipientStatus -notin ("Missing","Duplicate")})}
    }
    #refresh MR data for batch
    Get-MoveRequestReportData -Wave $wave -WaveType $wavetype -operation WaveMonitoring -ExchangeOrganization $ExchangeOrganization    
    #Common Move Request Parameters
    $MRParams = @{
        TargetDeliveryDomain = $CurrentOrgProfile.Office365Tenants.TargetDomain
        Remote = $true
        LargeItemLimit = $LargeItemLimit
        BadItemLimit = $BadItemLimit
        SuspendWhenReadyToComplete = $SuspendWhenReadyToComplete
		Suspend = $Suspend
        WarningAction = 'SilentlyContinue'
        ErrorAction = 'Stop'
        #consider adding parameters and values for CompleteAfter, ArchiveOnly, PrimaryOnly
        }
    #if ($StartTime -ne $null) {$MRParams.StartAfter = $StartTime} #experimental and not supported by microsoft.
    #Create Move Request in suspended state using -Suspend and perhaps -SuspendComment parameters
    $mraliases = ($Global:mr | Select-Object -expandproperty alias)
    $b = 0
    $RecordCount = $WaveData.Count
    Write-Log -Message "Found $RecordCount entries for $Wave in Source Data." -EntryType Notification -Verbose
    ForEach ($R in $WaveData) { 
        $b++
        Try {
            switch ($r.SourceSystem) {
                $_ {
                    $SourceSystem = $_
                    $MRParams.RemoteCredential = $CurrentOrgAdminProfileSystems | Where-Object {$_.SystemType -eq 'ExchangeOrganizations' -and $_.OrgType -eq 'OnPremises' -and $_.Name -eq $SourceSystem} | Select-Object -ExpandProperty Credential
                    if (-not [string]::IsNullOrWhiteSpace($r.MRSGroup)) 
                    {
                        $PotentialRemoteHostNameEndpoints = @(
                            $CurrentOrgProfile.ExchangeOrganizations | 
                            Where-Object {$_.name -eq $SourceSystem -and $_.OrgType -eq 'OnPremises'} | 
                            Select-Object -ExpandProperty MRSProxyServers | 
                            Where-Object -FilterScript {$_.Group -in $r.MRSGroup} |
                            Select-Object -ExpandProperty PublicFQDN
                        )
                    }
                    else 
                    {
                        $PotentialRemoteHostNameEndpoints = @(
                             $CurrentOrgProfile.ExchangeOrganizations | 
                             Where-Object {$_.name -eq $SourceSystem -and $_.OrgType -eq 'OnPremises'} | 
                             Select-Object -ExpandProperty MRSProxyServers | 
                             Where-Object -FilterScript {$_.IsDefault -eq $True} |
                             Select-Object -ExpandProperty PublicFQDN 
                         )
                    }
                    switch ($PotentialRemoteHostNameEndpoints.Count)
                    {
                        0 
                        {
                            $MRParams.RemoteHostName = $CurrentOrgProfile.ExchangeOrganizations | 
                             Where-Object {$_.name -eq $SourceSystem -and $_.OrgType -eq 'OnPremises'} | 
                             Select-Object -ExpandProperty MRSProxyServers | 
                             Where-Object -FilterScript {$_.IsDefault -eq $True}
                             Select-Object -ExpandProperty PublicFQDN | Select-Object -First 1
                        }
                        1
                        {
                            $MRParams.RemoteHostName = $PotentialRemoteHostNameEndpoints[0]
                        }
                        Default
                        {
                            $ZeroBasedEndpointCount = $PotentialRemoteHostNameEndpoints.Count
                            $UseEndpoint = Get-Random -Minimum 0 -Maximum $ZeroBasedEndpointCount 
                            $MRParams.RemoteHostName = $PotentialRemoteHostNameEndpoints[$UseEndpoint]
                        }
                    }
                } 
            }
            $identifier = $r.userPrincipalName
            Write-Progress -Activity "Creating or Verifying Move Requests" -Status "Processing Record $b of $RecordCount. Processing Request for user $identifier." -PercentComplete ($b/$RecordCount*100)  
            if ($R.Alias -notin $mraliases) {
                Connect-Exchange -ExchangeOrganization $ExchangeOrganization
                $LogString = "Creating Move Request for $identifier."
                Write-Log -Message $LogString -Verbose -EntryType Attempting -LogPath $LogPath
                $MRParams.Identity = $R.UserPrincipalName
                $MRParams.BatchName = $r.Wave
                $Global:ErrorActionPreference = 'Stop'
                Invoke-ExchangeCommand -splat $MRParams -cmdlet New-MoveRequest -ExchangeOrganization $ExchangeOrganization 
                #New-OLMoveRequest @MRParams 
                Write-Log -Message $logstring -verbose -EntryType Succeeded -LogPath $LogPath
                $Global:ErrorActionPreference = 'Continue'
            }
            else {
                Write-Log -Message "Move Request for $identifier already exists." -verbose -EntryType Notification -LogPath $LogPath
            }
        }#Try
        Catch {
            $Global:ErrorActionPreference = 'Continue'            
            Write-Log -Message $logstring -Verbose -ErrorLog  -ErrorLogPath $ErrorLogPath
            Write-Log -Message $_.tostring() -ErrorLog  -ErrorLogPath $ErrorLogPath
        }
    }#ForEach
}
function Set-MRMMoveRequestForCompletion
{
[cmdletbinding()]
param
(
    [string]$wave
    ,
    [parameter(Mandatory=$True)]
    [ValidateSet('Full','Sub')]
    [string]$wavetype
    ,
    [switch]$FailedOnly
    ,
    [int]$LargeItemLimit
    ,
    [int]$BadItemLimit
    ,
    [string]$LogFileBasePath = '-MoveRequestCompletionPreparation.log'
    , 
    [string]$ExchangeOrganization #convert to dynamic parameter 
)#Param
    [string]$stamp = Get-Date -Format yyyyMMdd-hhmm
    [string]$LogPath = ($trackingfolder + $stamp + '-' + $wave + $LogFileBasePath)
    [string]$ErrorLogPath = ($trackingfolder + $stamp + '-ERRORS-' + $wave + $LogFileBasePath)
    Write-Log -Message "Getting Move Request Data for Wave $Wave." -Verbose 
    Get-MoveRequestReportData -Wave $wave -WaveType $wavetype -operation WaveMonitoring -ExchangeOrganization $ExchangeOrganization
    if ($FailedOnly) {$ToProcess = $Global:fmr}
    else {$ToProcess = $Global:mr}
    #build parameter hash table
    $SMRQParams = @{}
    $SMRQParams.SuspendWhenReadyToComplete = $false
    if ($LargeItemLimit) {$SMRQParams.LargeItemLimit = $LargeItemLimit}
    if ($BadItemLimit) {$SMRQParams.BadItemLimit = $BadItemLimit}
    $SMRQParams.WarningAction = 'SilentlyContinue'
    $SMRQParams.ErrorAction = 'Stop'
    $SMRQParams.Confirm = $False
    $RecordCount = $ToProcess.count
    $b=0
    foreach ($request in $ToProcess) 
    {
        $b++
        $SMRQParams.Identity = $request.Exchangeguid.guid
        $logstring = "Set Properties of Move Request $($Request.DisplayName) for Completion Preparation"
        Write-Progress -Activity $logstring  -Status "Processing $($Request.DisplayName), record $b of $RecordCount." -PercentComplete ($b/$RecordCount*100)
        Connect-Exchange -ExchangeOrganization $ExchangeOrganization
        Try {
            Write-Log -Message $logstring -Verbose -EntryType Attempting
            Invoke-ExchangeCommand -ExchangeOrganization $ExchangeOrganization -cmdlet 'Set-MoveRequest' -splat $SMRQParams
            Write-Log -Message $logstring -Verbose -EntryType Succeeded
        }
        Catch {
            Write-Log -Message $logstring -Verbose -EntryType Failed -ErrorLog
            Write-Log -Message $_.tostring() -ErrorLog
        }
    }
}
function Start-MRMMoveRequestCompletion
{
[cmdletbinding()]
param
(
    [parameter(Mandatory = $true)]
    [string]$wave
    ,
    [parameter(Mandatory = $true)]
    [ValidateSet('Full','Sub')]
    [string]$wavetype
    ,
    [string]$LogFileBasePath = '-MoveRequestCompletion.log'
    ,
    [string]$MigrationBlockListFilePath
    , 
    [string]$ExchangeOrganization #convert to dynamic parameter 
    ,
    $SourceData = $Global:SourceData
)
[string]$stamp = Get-Date -Format yyyyMMdd_hhmm
[string]$LogPath = ($trackingfolder + $stamp + '-' + $wave + $LogFileBasePath)
[string]$ErrorLogPath = ($trackingfolder + $stamp + '-ERRORS-' + $wave + $LogFileBasePath)
switch ($wavetype)
{
        'Full' {$WaveSourceData = $SourceData | Where-Object {$_.Wave -match "\b$wave(\.\S*|\b)"}}
        'Sub' {$WaveSourceData = $SourceData | Where-Object {$_.wave -eq $wave}}
}
if ($MigrationBlockListFilePath) 
{
    Try
    {
            $MigrationBlockList = Import-Csv $MigrationBlockListFilePath -ErrorAction Stop
            $MigrationBlockListPSMTP = $MigrationBlockList | select-object -ExpandProperty PrimarySmtpAddress
    }
    Catch
    {
            $proceed = $false
            $_
    }
}

    #check for convergence of Move Requests and Wave Tracking
Get-MoveRequestReport -Wave $wave -WaveType $wavetype -operation WaveMonitoring -ExchangeOrganization $ExchangeOrganization
$mraliases = @($Global:mr | select-object -ExpandProperty Alias)
$wtaliases = @($WaveSourceData | Select-Object -ExpandProperty Alias) #PrimarySmtpAddress | Get-OPRecipient | Select-Object -ExpandProperty Alias
$unexpectedMR = @($mraliases | where-object {$_ -notin $wtaliases})
$missingMR = @($wtaliases | where-object {$_ -notin $mraliases})
$CountsMatch = ($WaveSourceData.count -eq $mr.Count)
    IF ($CountsMatch -and $unexpectedMR.count -eq 0 -and $missingMR.count -eq 0) {
        $proceed = $true
        Write-Log -Message "Migration Wave Tracking and Mailbox Move List Convergence Check PASSED" -Verbose
    }
    Else {
        $proceed = $false
        Write-Log -Verbose -errorlog -Message "ERROR: Migration Wave Tracking and Mailbox Move List Convergence Check FAILED" 
        Write-Output "Move Request Alias Count"
        Write-Output $mraliases.count
        Write-Output "Tracking data Alias Count"
        Write-Output $wtaliases.count
        Write-Output "Unexpected Move Requests"
        Write-Output $unexpectedMR
        Write-Output "Missing Move Requests"
        Write-Output $missingMR
    }

    if ($proceed) {
        $b = 0
        $RecordCount = $Global:mr.count
        foreach ($request in $WaveSourceData) {
            $b++
            Write-Progress -Activity "Processing move request resume for completion for all $wave move requests." -Status "Processing $($Request.PrimarySMTPAddress), record $b of $RecordCount." -PercentComplete ($b/$RecordCount*100)
            If ($request.PrimarySmtpAddress -notin $MigrationBlockListPSMTP) {
                Connect-Exchange -ExchangeOrganization $ExchangeOrganization
                Try {
                    $logstring = "Resume Move Request $($Request.PrimarySMTPAddress)"
                    Write-Log -Message $logstring -Verbose -EntryType Attempting
                    Resume-OLMoveRequest -Identity $request.PrimarySmtpAddress -ErrorAction Stop
                    Write-Log -Message $logstring -Verbose -EntryType Succeeded
                }
                Catch {
                    Write-Log -verbose -errorlog -Message $logstring -EntryType Failed
                    Write-Log -Message $_.tostring() -ErrorLog 
                }
            }
            Else {
                Write-Log -verbose -errorlog -Message "SKIPPED: Move Request $($Request.PrimarySmtpAddress) Found in Migration Block List." 
            }
        }
    }
    else {
        Write-Log -verbose -errorlog -Message "ERROR: Unable to Proceed with Move Request Completions for $wave because Migration Wave Tracking and Mailbox Move List Convergence Check FAILED" 
    }
}
function Get-MRMMoveRequestReport {
[cmdletbinding()]
param(
    [string]$Wave
    ,
    [parameter()]
    [ValidateSet('Full','Sub')]
    [string]$WaveType
    ,
    [parameter()]
    [ValidateSet('LargeItemReport','UpdateMigrationWaveTracking','WaveMonitoring','Offboarding')]
    [string]$operation
    ,
    [datetime]$FailedSince
    ,
    [parameter()]
    [ValidateSet('All','Failed','InProgress')]
    [string]$statsoperation
    ,
    [switch]$passthru
    ,
    [string]$ExchangeOrganization #convert to dynamic parameter later
)
Begin {
    function Get-MoveRequestForWave {
        switch ($WaveType) {
            'Full' {
                Connect-Exchange -ExchangeOrganization $ExchangeOrganization
                $Logstring = "Get all existing wave $wave move requests"
                Write-Log -message $Logstring -Verbose -EntryType Attempting 
                $Global:mr = @(Invoke-ExchangeCommand -cmdlet Get-MoveRequest -string "-BatchName $Wave* -ResultSize Unlimited" -ExchangeOrganization $ExchangeOrganization | where-object {$_.batchname -match "\b$wave(\.\S*|\b)"})
                #add error handling
            }
            'Sub' {
                Connect-Exchange -ExchangeOrganization $ExchangeOrganization
                $Logstring = "Get all existing sub wave $wave move requests"
                Write-Log -message $Logstring -Verbose -EntryType Attempting 
                $Global:mr = @(Invoke-ExchangeCommand -cmdlet Get-MoveRequest -string "-BatchName $Wave -ResultSize Unlimited" -ExchangeOrganization $ExchangeOrganization)
                #add error handling
            }
        }
        $Global:fmr = @($mr | ? {$_.status -eq 'Failed'})
        $Global:ipmr = @($mr | ? {$_.status -eq 'InProgress'})
        $Global:asmr = @($mr | ? {$_.status -eq 'AutoSuspended'})
        $Global:cmr = @($mr | ? {$_.status -like 'Completed*'})
        $Global:qmr = @($mr | ? {$_.status -eq 'Queued'})
    }
}
Process {
    switch ($operation) {
        'LargeItemReport' {
            Get-MoveRequestForWave
            $logstring = "Getting Statistics for all failed $wave move requests." 
            Write-Log -Message $logstring -EntryType Attempting -Verbose
            $RecordCount=$Global:fmr.count
            $b=0
            $Global:fmrs = @()
            foreach ($request in $fmr) {
                $b++
                Write-Progress -Activity $logstring -Status "Processing Record $b of $RecordCount." -PercentComplete ($b/$RecordCount*100)
                Connect-Exchange -ExchangeOrganization $ExchangeOrganization
                $Global:fmrs += Invoke-ExchangeCommand -cmdlet Get-MoveRequestStatistics -string "Identity $($request.exchangeguid)"
            }
            if ($failedsince) {
                $logstring =  "Getting Statistics for all large item failed $wave move requests, failed since $FailedSince."
                Write-Log -Message $logstring -EntryType Attempting -Verbose
                $slifmrs = @($Global:fmrs | ? {$_.FailureType -eq 'TooManyLargeItemsPermanentException' -and $_.FailureTimeStamp -gt $FailedSince})
                }                        
            else {
                $logstring =  "Getting Statistics for all large item failed $wave move requests."
                Write-Log -Message $logstring -EntryType Attempting -Verbose
                $slifmrs = @($Global:fmrs | ? {$_.FailureType -eq 'TooManyLargeItemsPermanentException'})
                }
            $RecordCount=$slifmrs.count
            $b=0
            $Global:lifmrs = @()
            foreach ($request in $slifmrs) {
                $b++
                Write-Progress -Activity "Getting move request statistics for all large item failed $wave move requests." -Status "Processing Record $b  of $RecordCount." -PercentComplete ($b/$RecordCount*100)
                Connect-Exchange -ExchangeOrganization $ExchangeOrganization
                $Global:lifmrs += $request | ForEach-Object {Invoke-ExchangeCommand -cmdlet Get-MoveRequestStatistics -string "-Identity $($_.alias) -IncludeReport" -ExchangeOrganization $ExchangeOrganization} | Select-Object -Property Alias,AllowLargeItems,ArchiveDomain,ArchiveGuid,BadItemLimit,BadItemsEncountered,BatchName,BytesTransferred,BytesTransferredPerMinute,CompleteAfter,CompletedRequestAgeLimit,CompletionTimestamp,DiagnosticInfo,Direction,DisplayName,DistinguishedName,DoNotPreserveMailboxSignature,ExchangeGuid,FailureCode,FailureSide,FailureTimestamp,FailureType,FinalSyncTimestamp,Flags,Identity,IgnoreRuleLimitErrors,InitialSeedingCompletedTimestamp,InternalFlags,IsOffline,IsValid,ItemsTransferred,LargeItemLimit,LargeItemsEncountered,LastUpdateTimestamp,MailboxIdentity,Message,MRSServerName,OverallDuration,PercentComplete,PositionInQueue,Priority,Protect,QueuedTimestamp,RecipientTypeDetails,RemoteArchiveDatabaseGuid,RemoteArchiveDatabaseName,RemoteCredentialUsername,RemoteDatabaseGuid,RemoteDatabaseName,RemoteGlobalCatalog,RemoteHostName,SourceArchiveDatabase,SourceArchiveServer,SourceArchiveVersion,SourceDatabase,SourceServer,SourceVersion,StartAfter,StartTimestamp,Status,StatusDetail,Suspend,SuspendedTimestamp,SuspendWhenReadyToComplete,SyncStage,TargetArchiveDatabase,TargetArchiveServer,TargetArchiveVersion,TargetDatabase,TargetDeliveryDomain,TargetServer,TargetVersion,TotalArchiveItemCount,TotalArchiveSize,TotalDataReplicationWaitDuration,TotalFailedDuration,TotalFinalizationDuration,TotalIdleDuration,TotalInProgressDuration,TotalMailboxItemCount,TotalMailboxSize,TotalProxyBackoffDuration,TotalQueuedDuration,TotalStalledDueToCIDuration,TotalStalledDueToHADuration,TotalStalledDueToMailboxLockedDuration,TotalStalledDueToReadCpu,TotalStalledDueToReadThrottle,TotalStalledDueToReadUnknown,TotalStalledDueToWriteCpu,TotalStalledDueToWriteThrottle,TotalStalledDueToWriteUnknown,TotalSuspendedDuration,TotalTransientFailureDuration,ValidationMessage,WorkloadType,@{n="BadItemList";e={@($_.Report.BadItems)}},@{n="LargeItemList";e={@($_.Report.LargeItems)}}
            }
            $logstring = "Getting Statistics for all communication error failed $wave move requests."
            Write-Log -Message $logstring -EntryType Attempting 
            $scefmrs = @($Global:fmrs | ? {$_.FailureType -eq 'CommunicationErrorTransientException'})
            $RecordCount=$scefmrs.count
            $b=0
            $Global:cefmrs = @()
            foreach ($request in $scefmrs) {
                $b++
                Write-Progress -Activity $logstring -Status "Processing Record $b of $RecordCount." -PercentComplete ($b/$RecordCount*100)
                Connect-Exchange -ExchangeOrganization $ExchangeOrganization
                $Global:cefmrs += $request | ForEach-Object {Invoke-ExchangeCommand -cmdlet Get-MoveRequestStatistics -string "-Identity $($_.alias) -IncludeReport" -ExchangeOrganization $ExchangeOrganization} | Select-Object -Property Alias,AllowLargeItems,ArchiveDomain,ArchiveGuid,BadItemLimit,BadItemsEncountered,BatchName,BytesTransferred,BytesTransferredPerMinute,CompleteAfter,CompletedRequestAgeLimit,CompletionTimestamp,DiagnosticInfo,Direction,DisplayName,DistinguishedName,DoNotPreserveMailboxSignature,ExchangeGuid,FailureCode,FailureSide,FailureTimestamp,FailureType,FinalSyncTimestamp,Flags,Identity,IgnoreRuleLimitErrors,InitialSeedingCompletedTimestamp,InternalFlags,IsOffline,IsValid,ItemsTransferred,LargeItemLimit,LargeItemsEncountered,LastUpdateTimestamp,MailboxIdentity,Message,MRSServerName,OverallDuration,PercentComplete,PositionInQueue,Priority,Protect,QueuedTimestamp,RecipientTypeDetails,RemoteArchiveDatabaseGuid,RemoteArchiveDatabaseName,RemoteCredentialUsername,RemoteDatabaseGuid,RemoteDatabaseName,RemoteGlobalCatalog,RemoteHostName,SourceArchiveDatabase,SourceArchiveServer,SourceArchiveVersion,SourceDatabase,SourceServer,SourceVersion,StartAfter,StartTimestamp,Status,StatusDetail,Suspend,SuspendedTimestamp,SuspendWhenReadyToComplete,SyncStage,TargetArchiveDatabase,TargetArchiveServer,TargetArchiveVersion,TargetDatabase,TargetDeliveryDomain,TargetServer,TargetVersion,TotalArchiveItemCount,TotalArchiveSize,TotalDataReplicationWaitDuration,TotalFailedDuration,TotalFinalizationDuration,TotalIdleDuration,TotalInProgressDuration,TotalMailboxItemCount,TotalMailboxSize,TotalProxyBackoffDuration,TotalQueuedDuration,TotalStalledDueToCIDuration,TotalStalledDueToHADuration,TotalStalledDueToMailboxLockedDuration,TotalStalledDueToReadCpu,TotalStalledDueToReadThrottle,TotalStalledDueToReadUnknown,TotalStalledDueToWriteCpu,TotalStalledDueToWriteThrottle,TotalStalledDueToWriteUnknown,TotalSuspendedDuration,TotalTransientFailureDuration,ValidationMessage,WorkloadType,@{n="TotalTransientFailureMinutes";e={@($_.TotalTransientFailureDuration.TotalMinutes)}},@{n="TotalStalledDueToMailboxLockedMinutes";e={@($_.TotalStalledDueToMailboxLockedDuration.TotalMinutes)}}
           }
        }
        'UpdateMigrationWaveTracking' {
            $logstring = "Getting all available move requests for Migration Wave Tracking Update"
            Write-Log -Message $logstring -EntryType Attempting
            Connect-Exchange -ExchangeOrganization $ExchangeOrganization
            $Global:mr = Invoke-ExchangeCommand -cmdlet Get-MoveRequest -string "-ResultSize Unlimited" -ExchangeOrganization $ExchangeOrganization
            $Global:fmr = @($mr | ? {$_.status -eq 'Failed'})
            $Global:ipmr = @($mr | ? {$_.status -eq 'InProgress'})
            $Global:asmr = @($mr | ? {$_.status -eq 'AutoSuspended'})
            $Global:cmr = @($mr | ? {$_.status -like 'Completed*'})
            $Global:qmr = @($mr | ? {$_.status -eq 'Queued'})
        }
        'WaveMonitoring' {
            Get-MoveRequestForWave
        }
        'Offboarding' {
            $logstring = "Getting all available offboarding move requests"
            Write-Log -Message $logstring -EntryType Attempting
            Connect-Exchange -ExchangeOrganization $ExchangeOrganization
            $Global:mr = Invoke-ExchangeCommand -cmdlet Get-MoveRequest -string "-ResultSize Unlimited" | where-object {$_.direction -eq 'Push'}
            $Global:fmr = @($mr | ? {$_.status -eq 'Failed'})
            $Global:ipmr = @($mr | ? {$_.status -eq 'InProgress'})
            $Global:asmr = @($mr | ? {$_.status -eq 'AutoSuspended'})
            $Global:cmr = @($mr | ? {$_.status -like 'Completed*'})
            $Global:qmr = @($mr | ? {$_.status -eq 'Queued'})     
        }
    }
    switch ($statsoperation) {
        'All' {
            $logstring = "Getting move request statistics for all $wave move requests." 
            Write-Log -Message $logstring -EntryType Attempting 
            $RecordCount=$Global:mr.count
            $b=0
            $Global:mrs = @()
            foreach ($request in $global:mr) {
                $b++
                Write-Progress -Activity $logstring -Status "Processing Record $b of $RecordCount." -PercentComplete ($b/$RecordCount*100)
                Connect-Exchange -ExchangeOrganization $ExchangeOrganization
                $Global:mrs += Invoke-ExchangeCommand -cmdlet Get-MoveRequestStatistics -string "-Identity $($request.exchangeguid)" -ExchangeOrganization $ExchangeOrganization
            }
            $Global:ipmrs = @($global:mrs | where-object {$psitem.status -like 'InProgress'})
            $Global:fmrs = @($global:mrs | where-object {$psitem.status -like 'Failed'})
            $global:cmrs = @($global:mrs |  where-object {$psitem.status -like 'Completed*'})
        }
        'Failed' {
            $logstring = "Getting Statistics for all failed $wave move requests."
            Write-Log -Message $logstring -EntryType Attempting
            $RecordCount=$Global:fmr.Count
            $b=0
            $Global:fmrs = @()
            foreach ($request in $fmr) {
                $b++
                Write-Progress -Activity $logstring -Status "Processing Record $b of $RecordCount." -PercentComplete ($b/$RecordCount*100)
                Connect-Exchange -ExchangeOrganization $ExchangeOrganization
                $
                $Global:fmrs += Invoke-ExchangeCommand -cmdlet Get-MoveRequestStatistics -string "-Identity $($request.exchangeguid)" -ExchangeOrganization $ExchangeOrganization
            }
        }
        'InProgress' {
            $logstring = "Getting Statistics for all in progress $wave move requests."
            Write-Log -Message $logstring -EntryType Attempting
            $RecordCount=$Global:ipmr.Count
            $b=0
            $Global:ipmrs = @()
            foreach ($request in $ipmr) {
                $b++
                Write-Progress -Activity $logstring -Status "Processing Record $b of $RecordCount." -PercentComplete ($b/$RecordCount*100)
                Connect-Exchange -ExchangeOrganization $ExchangeOrganization
                $Global:ipmrs += Invoke-ExchangeCommand -cmdlet Get-MoveRequestStatistics -string "-Identity $($request.exchangeguid)" -ExchangeOrganization $ExchangeOrganization
            }
        }
    }
}
End {
    if ($passthru) {
        $Global:mr 
    }
}
}
function Watch-MRMMoveRequest {
[cmdletbinding()]
param(
    [parameter()]
    [ValidateSet('Completion','Synchronization')]
    [string]$Operation
    ,
    [boolean]$ConfigureMailboxOptions
    ,
    [boolean]$ResumeAutosuspended
    ,
    [boolean]$ResumeFailed
    ,
    [parameter(Mandatory=$true)]
    [validateSet('Full','Sub')]
    [string]$wavetype = 'Sub'
    ,
    [parameter(Mandatory=$true)]
    [string]$wave
    ,
    [boolean]$MailNotification = $true
    ,
    [string[]]$Recipients
    ,
    [string]$Sender
    ,
    [string]$ExchangeOrganization
    ,
    $SourceData = $global:sourcedata
    #,
    #[switch]$SendUMWelcome
)
[string]$Stamp = Get-TimeStamp
#$LogFileBaseName = ('MonitorMoveRequest.log')
#$LogPath = $Global:LogFolderPath + $stamp + '-' + $wave + $LogFileBaseName
#$ErrorLogPath = $Global:LogFolderPath + $stamp + '-' + $wave + '-ERRORS' + $LogFileBaseName
if (-not (Test-Path 'variable:\WaveMigrationMonitoring')) {$Script:WaveMigrationMonitoring = @{}}
if ($Script:WaveMigrationMonitoring.$wave -eq 'Complete') {$MailNotification = $false}
switch ($wavetype) {
    'Full' {$WaveSourceData = $SourceData | Where-Object {$_.Wave -match "\b$wave(\.\S*|\b)"}}
    'Sub' {$WaveSourceData = $SourceData | Where-Object {$_.wave -eq $wave}}
}
Write-Log -message "Getting Migration Wave $wave Move Request Data." -Verbose 
Get-MoveRequestReportData -wave $wave -WaveType $wavetype -operation WaveMonitoring -statsoperation All -ExchangeOrganization $ExchangeOrganization
Write-Log -message "Received Migration Wave $wave Move Request Data." -Verbose 
if ($global:ipmrs.count -lt 1) {$Script:WaveMigrationMonitoring.$wave = 'Complete'} else {$Script:WaveMigrationMonitoring.$wave = 'InProgress'; $MailNotification = $true} 
if ($mailNotification -and $Global:mr.count -gt 0) {
    [string]$MessageTimeStamp = (Get-Date -Format 'yyyy-MM-dd HH:mm') + ' Eastern'
    $sendmailparams = @{}
    $sendmailparams.Subject = "Automatically Generated Message: Wave $wave Mailbox Move Status Update as of $MessageTimeStamp"
    #below needs to go in admin user profile or org profile
	$Sendmailparams.From = $Sender
    $Sendmailparams.To = $Recipients
    $Sendmailparams.SmtpServer = $CurrentOrgProfile.general.mailrelayserverFQDN
    $sendmailparams.BodyAsHtml = $true
    $sendmailparams.Attachments = ($Global:ExportDataPath + 'AllStatus.csv')
    #mail contents
    #table css
    $css = 
@"
<style type="text/css">
table {
	font-family: verdana,arial,sans-serif;
	font-size:11px;
	color:#333333;
	border-width: 1px;
	border-color: #666666;
	border-collapse: collapse;
}
table th {
	border-width: 1px;
	padding: 8px;
	border-style: solid;
	border-color: #666666;
	background-color: #dedede;
}
table td {
	border-width: 1px;
	padding: 8px;
	border-style: solid;
	border-color: #666666;
	background-color: #ffffff;
}
</style>
"@ 
    $IPR = $global:ipmrs | Select-Object DisplayName,Alias,BatchName,PercentComplete,TotalMailboxSize,TotalMailboxItemCount,ItemsTransferred,Status,StatusDetail,RemoteHostName | Sort-Object PercentComplete | ConvertTo-Html -As Table -Head $css
    $IPSR = $global:ipmrs | Select Status,StatusDetail | Group-Object StatusDetail | sort Name | Select @{n='Status Detail';e={$_.Name}},Count| ConvertTo-Html -as Table -Head $css
    $global:mrs | Select-Object MailboxIdentity,DisplayName,Alias,@{n='Wave';e={$_.Batchname}},Status,StatusDetail,PercentComplete,CompletionTimestamp | Sort-Object DisplayName | Export-Csv -NoTypeInformation -Force -Path ($Global:ExportDataPath + 'AllStatus.csv')
    if ($global:fmrs.count -ge 1)  {$FR = $global:fmrs | Select-Object DisplayName,Alias,BatchName,Status,StatusDetail,FailureType,FailureSide,FailureTimestamp | Sort-Object DisplayName | ConvertTo-Html -as Table -Head $css}
    $CR = $global:cmrs | Select-Object DisplayName,Alias,BatchName,PercentComplete,Status,StartTimeStamp,CompletionTimestamp | Sort-Object DisplayName | ConvertTo-Html -as Table -Head $css
if ($wavetype -eq 'Full') {
    $IPSRwS= $global:ipmrs | select Status,StatusDetail,BatchName | Group-Object BatchName,StatusDetail | sort Name | Select @{n='Sub Wave, Status Detail';e={$_.Name}},Count | ConvertTo-Html -As Table-Head $css
    $TMRwS = $global:mr | Group-Object BatchName | sort Name | Select @{n='Sub Wave';e={$_.Name}},Count | ConvertTo-Html -As Table -Head $css
    $TMRSwS = $global:mr | Group-Object BatchName,Status | sort Name | Select @{n='Sub Wave, Status';e={$_.Name}},Count| ConvertTo-Html -As Table -Head $css
}
    $Body = 
@"
<b>Wave $wave Mailbox Move $Operation has been Initiated.</b><br><br> 
Immediately following is summary information, followed by more detail per mailbox move. <br>
Attached in csv file format is status for each wave $wave mailbox move, current as of the generation of this message. <br><br> 
<b>Status summary for all $wave mailbox moves:</b><br>
Total Moves:`t $($Global:mr.count)<br>
Completed:`t $($Global:cmr.count)<br>
In Progress:`t $($Global:ipmr.count)<br>
Queued:`t $($Global:qmr.count)<br>
AutoSuspended: `t $($Global:asmr.count)<br>
Failed: `t $($Global:fmr.count)<br><br>
<b>Status Detail Summary for all $wave In Progress mailbox moves:</b><br>
$IPSR
<br><br>
"@ 

if ($wavetype -eq 'Full') {$body += 
@"
<b>Total moves per Sub Wave:</b>
$TMRwS
<br><br>

<b>Status Summary per Sub Wave $wave mailbox moves:</b><br>
<b>Move Status per Sub Wave:</b>
$TMRSwS
<br><br>

<b>Status Detail Summary per Sub Wave for all $wave In Progress mailbox moves:</b><br>
$IPSRwS
<br><br>
"@
}
if ($global:fmrs.count -ge 1) {$body +=
@"
<b>Failure details for currently Failed wave $wave mailbox moves:</b><br>
$FR
<br><br>
"@ 
}
$body += 
@"
<b>Statistics for currently In Progress wave $wave  mailbox moves:</b><br>
$IPR
<br><br>
<b>Completion details for all Completed wave $wave mailbox moves:</b><br>
$CR
<br><br>
"@ 

    $Sendmailparams.Body = $Body
    Send-MailMessage @sendmailparams
    Write-Log -message "Monitoring E-mail Message Sent to recipients $($Recipients -join ';') " -Verbose 
}
switch ($operation) {
    'Completion' {
        Write-Log -message "The active operation is $Operation" -Verbose 
        if ($ConfigureMailboxOptions) {Configure-MailboxOptions -Wave $wave -wavetype $wavetype -operation ExchangePostMigration}
        #resume any autosuspended requests
        if ($ResumeAutosuspended) {
            Write-Log -message "Attempting Resume Move Request for $($Global:asmr.count) Move Requests in Autosupsended state." -Verbose 
            foreach ($request in $global:asmr) {
                Connect-Exchange -ExchangeOrganization $ExchangeOrganization
                Try {
                    Invoke-ExchangeCommand -ExchangeOrganization $ExchangeOrganization -cmdlet 'Resume-MoveRequest' -string "-Identity $($request.exchangeguid.guid)" 
                    #Resume-OLMoveRequest -Identity $request.exchangeguid.guid
                }
                Catch {
                    Write-Log -message "Error: Failed to Resume Move Request for $($request.displayname)." -Verbose -ErrorLog
                    Write-Log -Message $_.tostring() -ErrorLog
                    $_
                }
            }
        }
        #resume any failed requests
        if ($ResumeFailed) {
            Write-Log -message "Attempting Resume Move Request for $($Global:fmr.count) Move Requests in Failed state." -Verbose 
            foreach ($request in $global:fmr) {
                Connect-Exchange -ExchangeOrganization $ExchangeOrganization
                Try {
                    Invoke-ExchangeCommand -ExchangeOrganization $ExchangeOrganization -cmdlet 'Resume-MoveRequest' -string "-Identity $($request.exchangeguid.guid)" 
                }
                Catch {
                    Write-Log -Verbose -message "Error: Failed to Resume Move Request for $($request.displayname)." 
                    Write-Log -Message $_.tostring() 
                    $_
                }
            }
        }
    }
    "Synchronization" {
        Write-Log -message "The active operation is $Operation" -Verbose 
        if ($ResumeAutosuspended) {
            Write-Log -message "Attempting Resume Move Request for $($Global:asmr.count) Move Requests in Autosuspended state." -Verbose 
            foreach ($request in $global:asmr) {
                Connect-Exchange -ExchangeOrganization $ExchangeOrganization
                Try {
                    Invoke-ExchangeCommand -ExchangeOrganization $ExchangeOrganization -cmdlet 'Resume-MoveRequest' -string "-Identity $($request.exchangeguid.guid)" 
                }
                Catch {
                    Write-Log -message "Error: Failed to Resume Move Request for $($request.displayname)." -Verbose 
                    Write-Log -Message $_.tostring() 
                    $_
                }
            }
        }
        #resume any failed requests
        if ($ResumeFailed) {
            Write-Log -message "Attempting Resume Move Request for $($Global:fmr.count) Move Requests in Failed state." -Verbose 
            foreach ($request in $global:fmr) {
                Connect-Exchange -ExchangeOrganization $ExchangeOrganization
                Try {
                    Invoke-ExchangeCommand -ExchangeOrganization $ExchangeOrganization -cmdlet 'Resume-MoveRequest' -string "-Identity $($request.exchangeguid.guid)" 
                }
                Catch {
                    Write-Log -Verbose -message "Error: Failed to Resume Move Request for $($request.displayname)." 
                    Write-Log -Message $_.tostring() 
                    $_
                }
            }
        }
    }
}
}
Function Watch-MRMMoveRequestContinuously {
    param(
    [parameter(Mandatory=$true)]
    [string]$Wave
    ,
    [parameter(Mandatory=$true)]
    [ValidateSet('Full','Sub')]
    [string]$WaveType = 'Sub'
    ,
    [datetime]$nextrun = (get-date)
    ,
    [int]$runperiod = 60
    ,
    [switch]$completion
    ,
    [switch]$resumeautosuspended
    ,
    [switch]$resumefailed
    ,
    [switch]$internal
    ,
    [string]$ExchangeOrganization #convert to Dynamic Parameter
    )
    while ($True) {
        $time = get-date 
        if ($time -ge $nextrun) { 
            $lastrunstart = get-date
            $nextrun = $nextrun.AddMinutes($runperiod)
            Connect-Exchange -ExchangeOrganization $ExchangeOrganization
            Write-Log "Running Watch-MoveRequest" -Verbose 
            $WMRParams = @{}
            if ($completion) {$WMRParams.Operation = 'Completion'} else {$WMRParams.Operation = 'Synchronization'}
            if ($resumeautosuspended) {$WMRParams.ResumeAutosuspended = $true}
            if ($resumefailed) {$WMRParams.ResumeFailed = $true}
            if ($internal) {$WMRParams.Internal = $true}
            $WMRParams.Wave = $Wave
            $WMRParams.WaveType = $WaveType
			$WMRParams.ExchangeOrganization = $ExchangeOrganization
            Watch-MoveRequest @WMRParams
            $lastruncompletion = get-date
        }
        $timeremaining = $nextrun - $time
        $minutes = $timeremaining.Minutes
        $hours = $timeremaining.Hours
        if (($Minutes % 15) -eq 0 -or ($minutes -le 5 -and $hours -eq 0)) {
            Write-Host "Last run of Watch-MoveRequest completed at $lastruncompletion. Next run of Watch-MoveRequest occurs in $minutes at $nextrun" -ForegroundColor DarkYellow
        }
        Start-Sleep -Seconds 60
    }
}
#New/Experimental:
function Start-MRMBackgroundWMRC {
[cmdletbinding()]
param
(
    [parameter(mandatory = $true)]
    $AdminUserProfileIdentity #Reccomended to use an admin user profile which logs in to minimally required systems
    ,
    [parameter(mandatory = $true)]
    $Wave
    ,
    [parameter(mandatory = $true)]
    [ValidationSet('Sub','Full')]
    $WaveType
    ,
    [switch]$Completion
    ,
    [switch]$ResumeFailed
    ,
    [int]$Runperiod #Run cycle in minutes
    ,
    [datetime]$nextrun #specify the first run date/time
)
$JobName = "Monitor $WaveType $Wave"
$startcomplexjobparams=
@{
    jobfunctions = @()
    name = $JobName
    arguments = @($AdminUserProfileIdentity,$Wave,$WaveType,$Completion,$ResumeFailed,$Runperiod,$nextrun)
    script = [scriptblock]{
        Import-Module OneShell
        Import-Module MoveRequestManagement
        #Initialize-AdminEnvironment -AdminUserProfileIdentity $Args[0] #need to allow specification of profiles
        $WMRCParams = 
            @{
                Wave = $Args[1]
                WaveType = $Args[2]
            }
        if ($Args[3] -eq $true) {$WMRCParams.Completion = $true}
        if ($Args[4] -eq $true) {$WMRCParams.ResumeFailed = $true}
        if ($Args[5] -ne $null) {$WMRCParams.Runperiod = $Args[5]}
        if ($Args[6] -ne $null) {$WMRCParams.nextrun = $Args[6]}
        Watch-MoveRequestContinuously @WMRCParams
    }#script
}#startcomplexjobparams
}
#not yet updated
function Get-MRMTrackingAndRequestConvergenceStatus {
[cmdletbinding()]
param(
[parameter(Mandatory = $True)]
[string]$wave
,
[parameter(Mandatory = $True)]
[ValidateSet('Full','Sub')]
[string]$wavetype
,[string]$LogFileBasePath = '_WaveBatchTrackingAndRequestConvergenceStatus.log'
,[switch]$includeBadADLookupStatus
,[string]$ExchangeOrganization
)

    [string]$stamp = Get-Date -Format yyyyMMdd_hhmm
    [string]$LogPath = ($trackingfolder + $stamp + '_' + $wave + $LogFileBasePath)
    [string]$ErrorLogPath = ($trackingfolder + $stamp + '_ERRORS_' + $wave + $LogFileBasePath)
    $SourceData = $Global:SourceData
    switch ($wavetype) {
        'Full' {
            if ($includeBadADLookupStatus) {
                $WaveData = $SourceData | Where-Object {$_.Wave -like "$wave*"}
            }
            else {$WaveData = $SourceData | Where-Object {$_.Wave -like "$wave*" -and $_.EKCStatus -notin ("Missing","NonUser","Ambiguous")}}
            }
        'Sub' {
            if ($includeBadADLookupStatus) {
                $WaveData = $SourceData | Where-Object {$_.Wave -match "\b$wave(\.\S*|\b)"}
            }
            else {$WaveData = $SourceData | Where-Object {$_.Wave -match "\b$wave(\.\S*|\b)" -and $_.EKCStatus -notin ("Missing","NonUser","Ambiguous")}}
            }
    }
    
    #check for convergence of Move Requests and Wave Tracking
    Get-MoveRequestReportData -Wave $wave -WaveType $wavetype -operation WaveMonitoring -ExchangeOrganization $exchangeOrganization
    $mraliases = $Global:mr | select-object -ExpandProperty Alias
    $wtaliases = $WaveData | ForEach-Object {switch ($psitem.sourcesystem) { 'Creo' {$psitem.CreMailNickname} 'KPG' {$psitem.KPGMailNickname}}}
    $unexpectedMR = @($mraliases | where-object {$_ -notin $wtaliases})
    $missingMR = @($wtaliases | where-object {$_ -notin $mraliases})
    $CountsMatch = ($WaveData.count -eq $mr.Count)
    IF ($CountsMatch -and $unexpectedMR.count -eq 0 -and $missingMR.count -eq 0) {
        $proceed = $true
        Write-Log -Message "Migration Wave Tracking and Mailbox Move List Convergence Check PASSED" -Verbose
        Write-Output "Batchname $Wave Move Request Count is $($mr.count)."
        Write-Output "Tracking List $Wave Record Count is $($WaveData.count)."

    }
    Else {
        $proceed = $false
        Write-Log -Message "ERROR: Migration Wave Tracking and Mailbox Move List Convergence Check FAILED" -Verbose -ErrorLog
        Write-Output "Batchname $Wave Move Request Count is $($mr.count)."
        Write-Output "Tracking List $Wave Record Count is $($WaveData.count)."
        if ($unexpectedMR.count -gt 0) {
            Write-Output "Unexpected Move Requests in Mailbox Move Request Batchname $wave :"
            Write-Output $unexpectedMR
        }
        if ($missingMR.count -gt 0) {
            Write-Output "Missing Move Requests in Mailbox Move Request Batchname $wave :"
            Write-Output $missingMR
        }
        
    }
}
function Get-MRMNonDeletedLargeItemReport {
[cmdletbinding()]
param(
    [string]$Wave
    ,[datetime]$FailedSince
    ,[switch]$SendMail
    ,[string]$ExchangeOrganization
)

[string]$Stamp = Get-Date -Format yyyyMMdd_HHmm
[string]$LargeItemReportFile = $trackingfolder + $Stamp + '_' + $wave + '_' + 'LargeItemReport.csv'
$LIReports = @()
    
#hash table for parameters for Get-MoveRequestReportData
$GetMRRD = @{}
$GetMRRD.Wave = $Wave
$GetMRRD.WaveType = 'Full'
$GetMRRD.Operation = 'LargeItemReport'
$GetMRRD.ExchangeOrganization = $ExchangeOrganization
if ($failedsince) {$GetMRRD.FailedSince = $FailedSince}


Get-MoveRequestReportData @GetMRRD

foreach ($request in $Global:lifmrs) {
    $DisplayName = $($request.MailboxIdentity.Rdn.UnescapedName)
    $FailureTimeStamp = $($request.FailureTimeStamp)
    $QualifiedLargeItems = @(
        $request.LargeItemList | ? {$_.WellKnownFolderType.tostring() -ne 'DumpsterDeletions'} | 
        foreach-object {"Subject: $($_.Subject); Folder: $($_.FolderName); Date: $($_.DateReceived); Sender: $($_.Sender); Recipient: $($_.Recipient); Size: $($_.MessageSize/1MB -as [int])MB"}
    )
    $LItemsNotDeletedList = $QualifiedLargeItems -join "`r`n"
    $QualifiedLargeItemCount = $QualifiedLargeItems.count
    If ($QualifiedLargeItemCount -gt 0) {
        Connect-Exchange -ExchangeOrganization $ExchangeOrganization
        $OLRecipientPrimarySmtpAddress = Get-OLRecipient -Identity $DisplayName | Select-Object -ExpandProperty PrimarySmtpAddress
        $LIReport = New-Object -TypeName PSObject -Property @{DisplayName = $DisplayName; PrimarySmtpAddress = $OLRecipientPrimarySmtpAddress; LargeOrBadItemCount = $QualifiedLargeItemCount; LargeOrBadItemList = $LItemsNotDeletedList; FailureTimeStamp = $FailureTimeStamp}
        $LIReports += $LIReport | Select-Object DisplayName,PrimarySmtpAddress,LargeOrBadItemCount,LargeOrBadItemList,FailureTimeStamp
    }
} 

if ($LIReports.count -gt 0) {
    $LIReports | Export-Csv -NoTypeInformation -Path $LargeItemReportFile -Append

    if ($SendMail) {
        Start-Sleep -Seconds 5

        $sendmailparams = @{}
        $sendmailparams.Cc = @('mike.campbell@perficient.com','jennifer.debner@perficient.com','tushar.shah@smith-nephew.com','mike.campbell@smith-nephew.com','clifford.cauley@perficient.com')
        $sendmailparams.To = @('david.margossian@smith-nephew.com')
        $sendmailparams.Attachments = $LargeItemReportFile
        $sendmailparams.From = $Global:MailNotificationSender
        $sendmailparams.Subject = "Large Item Report for Wave $Wave"
        $Sendmailparams.SmtpServer = $Global:MailRelayServer
        $sendmailparams.body = @"
David:  Please find attached the Large Item Report File for wave $wave.

Mike Campbell, Senior Consultant
o:312-589-2080  m:864-233-6174 | mike.campbell@perficient.com
"@

    Send-MailMessage @sendmailparams
    }
}
}
###################################################################################################
#pre/post migration configuration functions
###################################################################################################
function Set-MRMMailboxConfigurationOptions {
[cmdletbinding()]
param(
[parameter(mandatory=$true, parametersetname='MigrationWave')]
$wave
,
[parameter(mandatory=$true, parametersetname='MigrationWave')]
$wavetype
,
[parameter(mandatory=$true)]
[validateset('ExchangePostMigration','NotesPreMigration')]
$operation
,
[parameter(mandatory=$true, parametersetname='SingleUser')]
$UserPrincipalName
)
[string]$Stamp = Get-TimeStamp
$successfulconfigurations = @()
$failedconfigurations = @()
switch ($PSCmdlet.ParameterSetName) {
    'SingleUser' {
        [string]$logpath = $Global:trackingfolder + $Stamp + '-' + $UserPrincipalName + '-ConfigureMailboxOptions.log'
        [string]$errorlogpath = $Global:trackingfolder + $Stamp + '-' + $UserPrincipalName + '-ERRORS-ConfigureMailboxOptions.log'
        [string]$completionsfile = $Global:trackingfolder + $wave + '-MailboxConfigurationCompletionTracking.csv'
        $waveSourceData = @($SourceData | ? UserPrincipalName -eq $UserPrincipalName)
    }
    'MigrationWave' {
        [string]$logpath = $Global:trackingfolder + $Stamp + '-' + $Wave + '-ConfigureMailboxOptions.log'
        [string]$errorlogpath = $Global:trackingfolder + $Stamp + '-' + $Wave + '-ERRORS-ConfigureMailboxOptions.log'
        [string]$completionsfile = $Global:trackingfolder + $wave + '-MailboxConfigurationCompletionTracking.csv'
        switch ($wavetype) {
        'Full' {$WaveSourceData = @($SourceData | Where-Object {$_.Wave -match "\b$wave(\.\S*|\b)"})}
        'Sub' {$WaveSourceData = @($SourceData | Where-Object {$_.wave -eq $wave})}
        }
    }
}

if (-not $Global:ForwardingConfigurations) {
    Write-Log "Identifying most recent Forwarding Configurations File in Source Data Folder $global:ReferenceFolder"
    Try {
        $ForwardingConfigurationsFile = Get-ChildItem -Path $global:ReferenceFolder -Filter *ForwardingConfigurations.csv -ErrorAction stop | Sort-Object -Property LastWriteTime -Descending | Select-Object -First 1
        if ($ForwardingConfigurationsFile) {
            Write-Log -message "Most recent Forwarding Configurations File $($ForwardingConfigurationsFile.FullName) identified in Source Data Folder $global:ReferenceFolder" -Verbose 
            $Global:ForwardingConfigurations = Import-Csv $ForwardingConfigurationsFile.FullName -ErrorAction Stop}
        }
    Catch {
        Write-Log -message "ERROR: Unable to identify the most recent Forwarding Configurations File in Source Data Folder $global:ReferenceFolder" -ErrorLog
        $_
    }
}
if (-not $Global:SendAsConfigurations) {
    Write-Log "Identifying most recent Send As Configurations File in Source Data Folder $global:ReferenceFolder"
    Try {
        $SendAsConfigurationsFile = Get-ChildItem -Path $global:ReferenceFolder -Filter *SendAsConfigurations.csv -ErrorAction stop | Sort-Object -Property LastWriteTime -Descending | Select-Object -First 1
        if ($SendAsConfigurationsFile) {
            Write-Log -message "Most recent Send As Configurations File $($SendAsConfigurationsFile.FullName) identified in Source Data Folder $global:ReferenceFolder" -Verbose 
            $Global:SendAsConfigurations = Import-Csv $SendAsConfigurationsFile.FullName -ErrorAction Stop
        }
        }
    Catch {
        Write-Log -message "ERROR: Unable to identify the most recent Send As Configurations File in Source Data Folder $global:ReferenceFolder" -ErrorLog
        $_
    }
}
if (-not $Global:FullAccessConfigurations) {
    Write-Log "Identifying most recent Full Access Configurations File in Source Data Folder $global:ReferenceFolder"
    Try {
        $FullAccessConfigurationsFile = Get-ChildItem -Path $global:ReferenceFolder -Filter *FullAccessConfigurations.csv -ErrorAction stop | Sort-Object -Property LastWriteTime -Descending | Select-Object -First 1
        if ($FullAccessConfigurationsFile) {
            Write-Log -message "Most recent Full Access Configurations File $($FullAccessConfigurationsFile.FullName) identified in Source Data Folder $global:ReferenceFolder" -Verbose 
            $Global:FullAccessConfigurations = Import-Csv $FullAccessConfigurationsFile.FullName -ErrorAction Stop
        }
        }
    Catch {
        Write-Log -message "ERROR: Unable to identify the most recent Full Access Configurations File in Source Data Folder $global:ReferenceFolder" -ErrorLog
        $_
    }
}
Switch ($operation) {
    'ExchangePostMigration' {
        Write-Log -message "Beginning Configuration Operations for Completed Mailbox Moves" -Verbose -LogPath $LogPath       
        #record completed moves into input file 
        $completions = @($global:cmr | Select-Object DisplayName,DistinguishedName,ExchangeGuid,RecipientType,RecipientTypeDetails,Status)
        Write-Log -Message "Total Current Completion Count: $($completions.count)" -Verbose -LogPath $LogPath
        if ($completions.count -gt 0) {
            Write-Log -Message "Checking for existing Completions Tracking File $completionsfile and Importing if found." -Verbose -LogPath $LogPath
            #Find New Completions to Process by checking for an existing set of completions previously processed and exported
            $previouscompletions = IF (Test-Path $completionsfile) {Import-Csv -Path $completionsfile} Else {$false}
            $newcompletions=@()
            $RecordCount = $completions.Count
            $b = 0
            If ($previouscompletions) {
                Write-Log -Message "Existing Completions Tracking File $completionsfile was found and Imported." -Verbose -LogPath $LogPath
                $previouscompletionUPNs = $previouscompletions | Select-Object -ExpandProperty UserPrincipalName
                foreach ($c in $completions) {
                    $b++
                    Write-Progress -Activity "Finding new completions for $wave move requests." -Status "Processing Record $b of $RecordCount." -PercentComplete ($b/$RecordCount*100)
                    $MailboxMoveCompleted = $c.ExchangeGuid.Guid | get-olmailbox
                    if ($MailboxMoveCompleted.UserPrincipalName -notin $previouscompletionUPNs) {$newcompletions += $MailboxMoveCompleted}
                }
            }
            Else {
                foreach ($c in $completions) {
                    $b++
                    Write-Progress -Activity "Finding new completions for $wave move requests." -Status "Processing Record $b of $RecordCount." -PercentComplete ($b/$RecordCount*100)
                    $MailboxMoveCompleted = $c.ExchangeGuid.Guid | get-olmailbox
                    $newcompletions += $MailboxMoveCompleted
                }
            }
            #New Completions have been identified.  Now process changes and export completions to the configuration completion tracking file
            Write-Log -Message "Total New Completions Count Since Last Monitoring: $($newcompletions.count)" -Verbose -LogPath $LogPath
            if ($newcompletions.count -gt 0) {
                $RecordCount = $newcompletions.count
                $newcompletionsIdentities = $newcompletions | select -ExpandProperty UserPrincipalName
                $WaveSourceData = $WaveSourceData | ? UserPrincipalName -in $newcompletionsIdentities
            }
            else {$waveSourceData = $null}
        }
        else {$waveSourceData = $null}
    }
    'NotesPreMigration' {
    }
}
#Run the short running configurations first to get policies and Holds in place ASAP as well as admin permissions to mailboxes
$b=0
$RecordCount = $WaveSourceData.count
foreach ($SourceRecord in $WaveSourceData) {
    try {
                $configurationstatus = $true
                $b++
                Write-Progress -Activity "Processing Mailbox Configuration And Service Account Permissions for $wave Mailboxes." -Status "Processing Record $b of $RecordCount." -PercentComplete ($b/$RecordCount*100)
                $upn = $SourceRecord.UserPrincipalName
                Write-Log -Message "Mailbox Configuration for $upn is in progress." -Verbose -LogPath $LogPath
                Reconnect-ExchangeOnline
                #Set-Mailbox
                try {
                    $SetMailboxConfiguration = @{}
                    $SetMailboxConfiguration.Identity = $upn
                    if ($sourceRecord.LitigationHoldDesired -eq 'Y') {$SetMailboxConfiguration.LitigationHoldEnabled = $true}
                    $SetMailboxConfiguration.RetainDeletedItemsFor = 30
                    if (-not [string]::IsNullOrWhiteSpace($SourceRecord.RetentionPolicyDesired)) {$SetMailboxConfiguration.RetentionPolicy = $SourceRecord.RetentionPolicyDesired}
                    Write-Log -Message "Attempting: Set-Mailbox for $upn" -Verbose -LogPath $LogPath
                    Write-Log -Message "Parameters: $($SetMailboxConfiguration.keys)" -Verbose -LogPath $LogPath
                    Write-Log -Message "Values: $($SetMailboxConfiguration.values)" -Verbose -LogPath $LogPath
                    Set-olmailbox @SetMailboxConfiguration -erroraction Stop
                    Write-Log -Message "Success: Set-Mailbox for $upn." -Verbose -LogPath $LogPath
                }
                catch {
                    Write-Log -message "ERROR: Failed Set-Mailbox for $upn." -Verbose -errorlog $errorlogpath
                    Write-Log -message $_.tostring() -errorlog $errorlogpath
                    $configurationstatus = $false
                    $failedconfigurations += $SourceRecord
                }
                #Set-CASMailbox
                try {
                    $SetCASMailboxConfiguration = @{}
                    $SetCASMailboxConfiguration.Identity = $upn
                    $SetCASMailboxConfiguration.ImapEnabled = $false
                    $SetCASMailboxConfiguration.POPEnabled = $false
                    $SetCASMailboxConfiguration.OWAForDevicesEnabled = if ($sourceRecord.OWAForDevicesEnabled -eq 'Y') {$true} else {$false}
                    $SetCASMailboxConfiguration.ActiveSyncEnabled = $true #if ($sourceRecord.ActiveSyncEnabled -eq 'Y') {$true} else {$false}
                    if (-not [string]::IsNullOrWhiteSpace($SourceRecord.ActiveSyncPolicyDesired)) {$SetCASMailboxConfiguration.ActiveSyncMailboxPolicy = $SourceRecord.ActiveSyncPolicyDesired} 
                    Write-Log -Message "Attempting: Set-CASMailbox for $upn." -Verbose -LogPath $LogPath
                    Write-Log -Message "Parameters: $($SetCASMailboxConfiguration.keys)" -Verbose -LogPath $LogPath
                    Write-Log -Message "Values: $($SetCASMailboxConfiguration.values)" -Verbose -LogPath $LogPath
                    Set-olcasmailbox @SetCASMailboxConfiguration -erroraction Stop
                    Write-Log -Message "Success: Set-CASMailbox for $upn." -Verbose -LogPath $LogPath
                }
                catch {
                    Write-Log -message "ERROR: Failed Set-CASMailbox for $upn." -Verbose -errorlog $errorlogpath
                    Write-Log -message $_.tostring() -errorlog $errorlogpath
                    $configurationstatus = $false
                    $failedconfigurations += $SourceRecord
                }
                #Set Full Access Permissions for Service Group
                try {
                    Write-Log -Message "Attempting: Add-OLFullAccessPermissionForServiceGroup for $upn." -Verbose -LogPath $LogPath
                    Add-OLFullAccessPermissionForServiceGroup -identity $upn -erroraction Stop
                    Write-Log -Message "Success: Add-OLFullAccessPermissionForServiceGroup for $upn." -Verbose -LogPath $LogPath
                }
                catch {
                    Write-Log -message "ERROR: Failed Add-OLFullAccessPermissionForServiceGroup for $upn." -Verbose -errorlog $errorlogpath
                    Write-Log -message $_.tostring() -errorlog $errorlogpath
                    $configurationstatus = $false
                    $failedconfigurations += $SourceRecord
                }
                #Set Forwarding Configurations from Export
                try {
                    Write-Log -Message "Attempting: Set-OLForwardingConfiguration for $upn." -Verbose -LogPath $LogPath
                    Set-OLForwardingConfiguration -IdentityPrimarySmtpAddress $upn -ErrorAction Stop -logpath $logpath -errorlogpath $errorlogpath
                    Write-Log -Message "Success: Set-OLForwardingConfiguration for $upn." -Verbose -LogPath $LogPath
                }
                catch {
                    Write-Log -message "ERROR: Failed Set-OLForwardingConfiguration for $upn." -Verbose -errorlog $errorlogpath
                    Write-Log -message $_.tostring() -errorlog $errorlogpath
                    $configurationstatus = $false
                    $failedconfigurations += $SourceRecord
                }
                IF ($operation -eq 'ExchangePostMigration') {
                    Set-MailContactForwarding -UserPrincipalName $upn 
                }
            }
    catch {
            Write-Log -message $_.tostring() -errorlog $errorlogpath
            $configurationstatus = $false
        }
}

#Run the longer running configurations second
$b=0
$RecordCount = $WaveSourceData.count
foreach ($SourceRecord in $WaveSourceData) {
    try {
                $configurationstatus = $true
                $b++
                Write-Progress -Activity "Processing Mailbox and SendAS Permissions for $wave Mailboxes." -Status "Processing Record $b of $RecordCount." -PercentComplete ($b/$RecordCount*100)
                $upn = $SourceRecord.UserPrincipalName
                Write-Log -Message "Mailbox Configuration for $upn is in progress." -Verbose -LogPath $LogPath
                Reconnect-ExchangeOnline
                #Set Full Access Permissions from Export
                try {
                    Write-Log -Message "Attempting: Set-OLFullAccessPermissions for $upn." -Verbose -LogPath $LogPath
                    Set-OLFullAccessPermissions -SingleMailbox -IdentityPrimarySmtpAddress $upn -ErrorAction Stop -logpath $logpath -errorlogpath $errorlogpath
                    Write-Log -Message "Success: Set-OLFullAccessPermissions for $upn." -Verbose -LogPath $LogPath
                }
                catch {
                    Write-Log -message "ERROR: Failed Set-OLFullAccessPermissions for $upn." -Verbose -errorlog $errorlogpath
                    Write-Log -message $_.tostring() -errorlog $errorlogpath
                    $configurationstatus = $false
                    $failedconfigurations += $SourceRecord
                }
                #Set-OLFullAccessPermissionsToMirrorSendAS -SingleMailbox -IdentityPrimarySmtpAddress $SourceRecord.PrimarySmtpAddress
                if ($configurationstatus) {
                    $successfulconfigurations += $SourceRecord
                }
            }
    catch {
            Write-Log -message $_.tostring() -errorlog $errorlogpath
            $configurationstatus = $false
        }
}
Write-Log -Message "Attempting Export of Successful Mailbox Configurations to Configuration Completions Tracking File: $completionsfile." -Verbose -LogPath $LogPath
$successfulconfigurations | Export-csv -Append -Path $completionsfile -NoTypeInformation 
if ($failedconfigurations.count -gt 0) {
    Write-Log -Message "Attempting Export of Failed Mailbox Configurations to Tracking Folder $Global:trackingfolder." -Verbose -LogPath $LogPath
    Export-Data -DataToExportTitle MailboxConfigurationFailures -DataToExport $failedconfigurations -datatype csv -exportFolderPath $Global:trackingfolder
}
}
function Set-MRMFullAccessPermissions {
[cmdletbinding()]
param(
    [switch]$SingleMailbox
    ,
    [string]$IdentityPrimarySmtpAddress
    ,
    $logpath
    ,
    $errorlogpath
    ,
    [switch]$automapping
)
if (-not $logpath -or -not $errorlogpath) {
    $stamp = get-timestamp
    $LogPath = $trackingfolder + $stamp + '-AddOLFullAccessPerms.log'
    $ErrorLogPath = $trackingfolder + $stamp + '-ERRORS-AddOLFullAccessPerms.log'
}
if (-not $Global:FullAccessConfigurations) {
    Write-Log "Identifying most recent Full Access Configurations File in Source Data Folder $global:ReferenceFolder"
    Try {
        $FullAccessConfigurationsFile = Get-ChildItem -Path $global:ReferenceFolder -Filter *FullAccessConfigurations.csv -ErrorAction stop | Sort-Object -Property LastWriteTime -Descending | Select-Object -First 1
        Write-Log -message "Most recent Full Access Configurations File $($FullAccessConfigurationsFile.FullName) identified in Source Data Folder $global:ReferenceFolder" -Verbose 
        $Global:FullAccessConfigurations = Import-Csv $FullAccessConfigurationsFile.FullName -ErrorAction Stop
        }
    Catch {
        Write-Log -message "ERROR: Unable to identify the most recent Full Access Configurations File in Source Data Folder $global:ReferenceFolder" -ErrorLog
        $_
    }
}
if ($SingleMailbox) {
    $FullaccessPerms = @($Global:FullAccessConfigurations | ? IdentityPrimarySmtpAddress -eq $IdentityPrimarySmtpAddress)
}
else {$FullaccessPerms = $Global:FullAccessConfigurations}
$RecordCount = $FullaccessPerms.Count
$b=0
if ($RecordCount -gt 0) {
    foreach ($perm in $FullaccessPerms) {
        Reconnect-ExchangeOnline
        $b++
        Write-Progress -Activity "Granting FullAccess Permissions in Exchange Online from Full Access Configurations Export." -Status "Processing Record $b of $RecordCount" -PercentComplete ($b/$recordcount*100) 
        Try {
            Write-Log -Message "Attempt: Grant Permission to $($Perm.UserPrimarySmtpAddress) to $($Perm.AccessRights) $($perm.IdentityPrimarySmtpAddress)." -logpath $logpath
            Add-OLMailboxPermission -AccessRights FullAccess -Identity $perm.IdentityPrimarySmtpAddress -User $perm.UserPrimarySmtpAddress -Confirm:$False -ErrorAction Stop -AutoMapping $automapping
            Write-Log -Message "Success: Grant Permission to $($Perm.UserPrimarySmtpAddress) to $($Perm.AccessRights) $($perm.IdentityPrimarySmtpAddress)."-logpath $logpath
        }
        Catch {
            Write-Log -verbose -errorlog -Message "ERROR: Failed Grant Permission to $($Perm.UserPrimarySmtpAddress) to $($Perm.AccessRights) $($perm.IdentityPrimarySmtpAddress)." -LogPath $LogPath -ErrorLogPath $ErrorLogPath
            Write-Log -Message $_.tostring() -LogPath $ErrorLogPath
        }
    }
        Write-Progress -Activity "Granting FullAccess Permissions in Exchange Online to Mirror Exchange On Prem SendAS Permissions." -Status "Processing Record $b of $RecordCount" -Completed
}
}
function Set-MRMForwardingConfiguration {
[cmdletbinding()]
param(
[parameter(Mandatory=$True,ValueFromPipeline = $true)]
$IdentityPrimarySmtpAddress
,
$logpath
,
$errorlogpath
)
#Load Forwarding Configurations File into memory if needed
    if (-not $Global:ForwardingConfigurations) {
        Write-Log "Identifying most recent Forwarding Configurations File in Source Data Folder $global:ReferenceFolder"
        Try {
            $ForwardingConfigurationsFile = Get-ChildItem -Path $global:ReferenceFolder -Filter *ForwardingConfigurations.csv -ErrorAction stop | Sort-Object -Property LastWriteTime -Descending | Select-Object -First 1
            Write-Log -message "Most recent Forwarding Configurations File $($ForwardingConfigurationsFile.FullName) identified in Source Data Folder $global:ReferenceFolder" -Verbose 
            If ($ForwardingConfigurationsFile) {$Global:ForwardingConfigurations = Import-Csv $ForwardingConfigurationsFile.FullName -ErrorAction Stop}
            }
        Catch {
            Write-Log -message "ERROR: Unable to identify the most recent Forwarding Configurations File in Source Data Folder $global:ReferenceFolder" -ErrorLog
            $_
        }
    }
    if (-not $global:ForwardingConfigurationsIdentities) {
       $global:ForwardingConfigurationsIdentities = $Global:ForwardingConfigurations | select -ExpandProperty Identity
    }

    IF ($IdentityPrimarySmtpAddress -in $global:ForwardingConfigurationsIdentities) {
        try {
            $forwardingconfig = $Global:ForwardingConfigurations | ? Identity -eq $IdentityPrimarySmtpAddress
            $forwardingparams = @{'Identity' = $forwardingconfig.Identity}
            $forwardingparams.ErrorAction = 'Stop'
            if ($forwardingconfig.ForwardingAddress) {$forwardingparams.ForwardingAddress = $forwardingconfig.ForwardingAddress}
            if ($forwardingconfig.ForwardingSmtpAddress) {$forwardingparams.ForwardingSmtpAddress = $forwardingconfig.ForwardingSmtpAddress}
            if ($forwardingconfig.DeliverToMailboxAndForward -eq 'TRUE') {$forwardingparams.DeliverToMailboxAndForward = $true}
            write-log -message "Attempt: Set Forwarding Configuration for $identityPrimarySmtpAddress." -logpath $logpath
            Set-OLmailbox @forwardingparams 
            write-log -message "Success: Set Forwarding Configuration for $identityPrimarySmtpAddress." -logpath $logpath
        }
        catch {
            write-log -message "ERROR: Fail to Set Forwarding Configuration for $identityPrimarySmtpAddress." -logpath $logpath -errorlogpath $errorlogpath -errorlog -verbose
            write-log -message $_.tostring() -errorlogpath $errorlogpath -errorlog 
        }
    }
    Else {Write-Log -message "No Forwarding Configuration Found for $identityPrimarySmtpAddress." -logpath $logpath -Verbose}
}
Function Set-MRMMailboxQuotas {
[cmdletbinding()]
Param(
[parameter(Mandatory=$True,Position=0)]
[string]$Identity
,
[parameter(Mandatory=$True,Position=1)]
[ValidateSet('E4''E3','E2','E1','K1','Resource','Shared','None')]
[string]$Quotas
,
$logpath
,
$errorlogpath
)
Begin {}
Process{
    #Quota, Retention Policy, and Deleted Item Retention Configuration
    $Message = "Setting for $Identity : $LicenseTypeDesired Quotas"
    if ($LogPath) {Write-Log -Message $message -Verbose -LogPath $LogPath}
    else {Write-Host $message -ForegroundColor Cyan}
        
    $SetMailboxParams = @{}
    $SetMailboxParams.Identity = $Identity
    Switch ($Quotas) {
        'E4' {$SetMailboxParams = $SetMailboxParams + $Global:E3Quotas}
        'E3' {$SetMailboxParams = $SetMailboxParams + $Global:E3Quotas}
        'E2' {$SetMailboxParams = $SetMailboxParams + $Global:E2Quotas}
        'E1' {$SetMailboxParams = $SetMailboxParams + $Global:E1Quotas}
        'K1' {$SetMailboxParams = $SetMailboxParams + $Global:K1Quotas}
        'Resource' {$SetMailboxParams = $SetMailboxParams + $Global:SRQuotas}
        'Shared' {$SetMailboxParams = $SetMailboxParams + $Global:SRQuotas}
        'None' {$SetMailboxParams = $SetMailboxParams + $Global:SRQuotas}
        Default {$SetMailboxParams = $SetMailboxParams + $Global:SRQuotas}            
    }
    Try {
        Set-OlMailbox @SetMailboxParams 
    }
    Catch {
            Write-Log -Message "Error: Failed Setting Mailbox Quotas for $Identity" -Verbose -ErrorLog
            Write-Log -Message $_.tostring() -ErrorLog 
            $_
    }
}
End {}
}