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
    $SourceData = $Script:sourcedata
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
    [string]$ExchangeOrganization #Target Exchange Organization or Online if doing offboarding back to on premises
  )
    #Get Endpoints and Credential Data from OneShell
    $CurrentOrgAdminProfileSystems = Get-OneShellVariableValue -Name CurrentOrgAdminProfileSystems
    $CurrentOrgProfile = Get-OneShellVariableValue -Name CurrentOrgProfile
    switch ($wavetype) 
    {
        'Full' {$WaveData = @($SourceData | Where-Object {$_.Wave -match "\b$wave(\.\S*|\b)"})} #-and $_.RecipientStatus -notin ("Missing","Duplicate")})}
        'Sub' {$WaveData = @($SourceData | Where-Object {$_.Wave -eq $wave})} #-and $_.RecipientStatus -notin ("Missing","Duplicate")})}
    }
    #refresh MR data for batch
    $MR = @(Get-MRMMoveRequestReport -Wave $wave -WaveType $wavetype -operation WaveMonitoring -ExchangeOrganization $ExchangeOrganization -passthru)
    $CurrentOrgProfile = Get-OneShellVariableValue -Name CurrentOrgProfile     
    #Common Move Request Parameters
    $MRParams = @{
        TargetDeliveryDomain = $CurrentOrgProfile.Office365Tenants[0].TargetDomain #need to fix logic in case there is more than one tenant
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
    #$mraliases = ($Script:mr | Select-Object -expandproperty alias)
    if ($mr.count -ge 1)
    {
      $MRIdentifiersLookup = $MR | Group-Object -AsHashTable -AsString -Property ExchangeGuid
    }
    else {$MRIdentifiersLookup = @{}}
    $b = 0
    $RecordCount = $WaveData.Count
    $CurrentOrgAdminProfileSystems = Get-OneShellVariableValue -Name CurrentOrgAdminProfileSystems
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
            if (-not $MRIdentifiersLookup.ContainsKey($R.ExchangeGuid)) {
                Connect-Exchange -ExchangeOrganization $ExchangeOrganization > $null
                $Message = "Create Move Request for $identifier."
                Write-Log -Message $Message -Verbose -EntryType Attempting -LogPath $LogPath
                $MRParams.Identity = $R.ExchangeGuid
                $MRParams.BatchName = $r.Wave
                $Global:ErrorActionPreference = 'Stop'
                Invoke-ExchangeCommand -splat $MRParams -cmdlet New-MoveRequest -ExchangeOrganization $ExchangeOrganization 
                #New-OLMoveRequest @MRParams 
                Write-Log -Message $Message -verbose -EntryType Succeeded -LogPath $LogPath
                $Global:ErrorActionPreference = 'Continue'
            }
            else {
                $message = "Move Request for $identifier in Wave $($r.wave) already exists."
                Write-Log -Message $message -verbose -EntryType Notification -LogPath $LogPath
            }
        }#Try
        Catch {
            $Global:ErrorActionPreference = 'Continue'            
            Write-Log -Message $Message -Verbose -ErrorLog  -ErrorLogPath $ErrorLogPath
            Write-Log -Message $_.tostring() -ErrorLog  -ErrorLogPath $ErrorLogPath
        }
    }#ForEach
}#New-MRMMoveRequest
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
    [int]$LargeItemLimit
    ,
    [int]$BadItemLimit
    ,
    [string]$ExchangeOrganization #convert to dynamic parameter
    ,
    [switch]$ByPassConvergenceCheck
    ,
    $SourceData
)#Param
switch ($wavetype)
{
        'Full' {$WaveSourceData = $SourceData | Where-Object {$_.Wave -match "\b$wave(\.\S*|\b)"}}
        'Sub' {$WaveSourceData = $SourceData | Where-Object {$_.wave -eq $wave}}
}
#check for convergence of Move Requests and Wave Tracking
if ($ByPassConvergenceCheck) 
{
    Write-Log -Message "WARNING:Migration Tracking Database and Move Request Convergence for $Wave has been bypassed." -EntryType Notification -Verbose
    $proceed = $true
}
else {
    $TestConvergenceParams =
    @{
        Wave = $wave
        WaveType = $wavetype
        ExchangeOrganization = $ExchangeOrganization
        IncludeBadADLookupStatus = $IncludeBadADLookkupStatusInConvergenceCheck
        Report = 'All'
        SourceData = $SourceData
    }
    $proceed = Test-MRMConvergence @TestConvergenceParams
}
if ($proceed -eq $True)
{
    #build parameter hash table
    $SMRQParams = @{}
    $SMRQParams.SuspendWhenReadyToComplete = $false
    if ($LargeItemLimit) {$SMRQParams.LargeItemLimit = $LargeItemLimit}
    if ($BadItemLimit) {$SMRQParams.BadItemLimit = $BadItemLimit}
    $SMRQParams.WarningAction = 'SilentlyContinue'
    $SMRQParams.ErrorAction = 'Stop'
    $SMRQParams.Confirm = $False
    $RecordCount = $WaveSourceData.count
    $b=0
    foreach ($request in $WaveSourceData) 
    {
        $b++
        $SMRQParams.Identity = $request.ExchangeGuid
        $logstring = "Set Properties of Move Request $($Request.UserPrincipalName) for Completion Preparation"
        Write-Progress -Activity $logstring  -Status "Processing $($Request.UserPrincipalName), record $b of $RecordCount." -PercentComplete ($b/$RecordCount*100)
        Connect-Exchange -ExchangeOrganization $ExchangeOrganization > $null
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
else
{
    Write-Log -verbose -errorlog -Message -EntryType Failed "Unable to Set Move Requests for Completion for $wave because Convergence Check FAILED"
}
}#Set-MRMMoveRequestForCompletion
function Test-MRMConvergence
{
[cmdletbinding()]
param
(
[parameter(Mandatory = $True)]
[string]$wave
,
[parameter(Mandatory = $True)]
[ValidateSet('Full','Sub')]
[string]$wavetype
,
[switch]$includeBadADLookupStatus
,
[parameter(Mandatory)]
$SourceData
,
[string]$ExchangeOrganization
,
[parameter()]
[validateset('All','Missing','Unexpected')]
$Report
)
switch ($wavetype) 
{
    'Full' 
    {
            if ($includeBadADLookupStatus) {
                $WaveData = $SourceData | Where-Object {$_.Wave -like "$wave*"}
            }
            else {$WaveData = $SourceData | Where-Object {$_.Wave -like "$wave*" -and $_.ADStatus -notin ("Missing","NonUser","Ambiguous")}}
    }#'Full'
    'Sub' 
    {
            if ($includeBadADLookupStatus) {
                $WaveData = $SourceData | Where-Object {$_.Wave -match "\b$wave(\.\S*|\b)"}
            }
            else {$WaveData = $SourceData | Where-Object {$_.Wave -match "\b$wave(\.\S*|\b)" -and $_.ADStatus -notin ("Missing","NonUser","Ambiguous")}}
    }#'Sub'
}#switch
#check for convergence of Move Requests and Wave Tracking
Get-MRMMoveRequestReport -Wave $wave -WaveType $wavetype -operation WaveMonitoring -ExchangeOrganization $ExchangeOrganization
$MoveRequests = $Script:mr
$MRLookupHash = @($Script:mr | Group-Object -AsHashTable -AsString -Property ExchangeGuid)
$WTLookupHash = @($WaveData | Group-Object -AsHashTable -AsString -Property ExchangeGuid)
$UnexpectedMR = @(
  $MoveRequests | Where-Object -FilterScript {-not $WTLookupHash.containskey($_.ExchangeGuid.guid)}
)
$MissingMR = @(
  $WaveData | Where-Object -FilterScript {-not $MRLookupHash.containskey($_.ExchangeGuid)}
)
$CountsMatch = ($WaveData.count -eq $MoveRequests.Count)
If ($CountsMatch -and $UnexpectedMR.count -eq 0 -and $MissingMR.count -eq 0) 
{
    Write-Log -Message "Migration Wave Tracking and Mailbox Move List Convergence Check PASSED" -Verbose
    $true
}#If
Else 
{
    Write-Log -Verbose -errorlog -Message "ERROR: Migration Wave Tracking and Mailbox Move List Convergence Check FAILED" 
    Write-Log "Move Request Count: $($MoveRequests.count )" -Verbose -ErrorLog
    Write-Log "Tracking data Alias Count: $($WaveData.count)" -Verbose -ErrorLog
    Write-Log "Unexpected Move Requests Count: $($UnexpectedMR.Count)" -Verbose -ErrorLog
    Write-Log "Missing Move Requests Count: $($MissingMR.Count)" -Verbose -ErrorLog
    switch ($Report)
    {
      NULL {$false}
      'All'
      {
        [pscustomobject]@{
            MissingMoveRequests = $MissingMR
            UnexpectedMoveRequests = $UnexpectedMR
        }
      }
      'Missing' {$MissingMR}
      'Unexpected' {$UnexpectedMR}
    }
}#Else
}#function Test-MRMTrackingListAndRequestConvergence
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
    [string[]]$MigrationBlockList
    , 
    [string]$ExchangeOrganization #convert to dynamic parameter 
    ,
    $SourceData = $Script:SourceData
    ,
    [switch]$ByPassConvergenceCheck
    ,
    [switch]$IncludeBadADLookkupStatusInConvergenceCheck

)
switch ($wavetype)
{
        'Full' {$WaveSourceData = $SourceData | Where-Object {$_.Wave -match "\b$wave(\.\S*|\b)"}}
        'Sub' {$WaveSourceData = $SourceData | Where-Object {$_.wave -eq $wave}}
}
#check for convergence of Move Requests and Wave Tracking
if ($ByPassConvergenceCheck) 
{
    Write-Log -Message "WARNING:Migration Tracking Database and Move Request Convergence for $Wave has been bypassed." -EntryType Notification -Verbose
    $proceed = $true
}
else {
    $TestConvergenceParams =
    @{
        Wave = $wave
        WaveType = $wavetype
        ExchangeOrganization = $ExchangeOrganization
        IncludeBadADLookupStatus = $IncludeBadADLookkupStatusInConvergenceCheck
        Report = 'All'
        SourceData = $SourceData
    }
    $proceed = Test-MRMConvergence @TestConvergenceParams
}
#If Convergence checks out or Bypass Convergence was chosen, proceed with move request completions.  
if ($proceed -eq $true) {
    $b = 0
    $RecordCount = $Script:mr.count
    $RMRParams = @{
      ErrorAction = 'Stop'
    }
    foreach ($request in $WaveSourceData) {
        $b++
        Write-Progress -Activity "Processing move request resume for completion for all $wave move requests." -Status "Processing $($Request.PrimarySMTPAddress), record $b of $RecordCount." -PercentComplete ($b/$RecordCount*100)
        If ($request.PrimarySmtpAddress -notin $MigrationBlockListPSMTP) {
            Connect-Exchange -ExchangeOrganization $ExchangeOrganization > $null
            Try {
                $logstring = "Resume Move Request $($Request.PrimarySMTPAddress) with Exchange Guid $($request.ExchangeGuid) for Completion."
                Write-Log -Message $logstring -Verbose -EntryType Attempting
                $RMRParams.Identity = $request.ExchangeGuid
                Invoke-ExchangeCommand -cmdlet 'Resume-MoveRequest' -ExchangeOrganization $ExchangeOrganization -splat $RMRParams
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
    Write-Log -verbose -errorlog -Message "ERROR: Unable to Proceed with Move Request Completions for $wave because Convergence Check FAILED" 
}
}#function Start-MRMMoveRequestCompletion
function Resume-MRMMoveRequestForDeltaSync
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
    [string]$ExchangeOrganization #convert to dynamic parameter
    ,
    $SourceData = $Script:SourceData
    ,
    [switch]$ByPassConvergenceCheck
    ,
    [switch]$IncludeBadADLookkupStatusInConvergenceCheck
    ,
    [switch]$FailedOnly
)
switch ($wavetype)
{
        'Full' {$WaveSourceData = $SourceData | Where-Object {$_.Wave -match "\b$wave(\.\S*|\b)"}}
        'Sub' {$WaveSourceData = $SourceData | Where-Object {$_.wave -eq $wave}}
}
#check for convergence of Move Requests and Wave Tracking
if ($ByPassConvergenceCheck)
{
    Write-Log -Message "WARNING:Migration Tracking Database and Move Request Convergence for $Wave has been bypassed." -EntryType Notification -Verbose
    $proceed = $true
}
else {
    $TestConvergenceParams =
    @{
        Wave = $wave
        WaveType = $wavetype
        ExchangeOrganization = $ExchangeOrganization
        IncludeBadADLookupStatus = $IncludeBadADLookkupStatusInConvergenceCheck
        SourceData = $SourceData
        Report = 'All'
    }
    $proceed = Test-MRMConvergence @TestConvergenceParams
}
#If Convergence checks out or Bypass Convergence was chosen, proceed with move request delta synchronizations.
if ($proceed -eq $true)
{
    $b = 0
    $RecordCount = $WaveSourceData.count
    $RMRParams = @{
      ErrorAction = 'Stop'
      SuspendWhenReadyToComplete = $true
    }
    if ($FailedOnly)
    {
      if (-not $ByPassConvergenceCheck)
      {
        $FMRLookupHashByExchangeGuid = $script:fmr | Group-Object -AsHashTable -AsString -Property ExchangeGuid 
      }
      else
      {
        $fmr = Get-MRMMoveRequestReport -Wave $wave -WaveType $wavetype -operation WaveMonitoring -ExchangeOrganization $ExchangeOrganization -passthru | Where-Object -FilterScript {$_.status -like '*fail*'}
        $FMRLookupHashByExchangeGuid = $fmr | Group-Object -AsHashTable -AsString -Property ExchangeGuid 
      }
    }
    foreach ($request in $WaveSourceData)
    {
        $b++
        Write-Progress -Activity "Processing move request resume for delta sync for all $wave move requests." -Status "Processing $($Request.UserPrincipalName), record $b of $RecordCount." -PercentComplete ($b/$RecordCount*100)
        if ($FailedOnly -and $FMRLookupHashByExchangeGuid.Count -ge 1)
        {
          if (-not $FMRLookupHashByExchangeGuid.ContainsKey($request.ExchangeGuid))
          {Continue}
        }
        elseif ($FailedOnly)
        {Continue}
        Try
        {
            Connect-Exchange -ExchangeOrganization $ExchangeOrganization > $null
            $logstring = "Resume Move Request $($Request.UserPrincipalName) with Exchange GUID $($request.ExchangeGuid) for Delta Sync."
            Write-Log -Message $logstring -Verbose -EntryType Attempting
            $RMRParams.Identity = $request.ExchangeGuid
            Invoke-ExchangeCommand -cmdlet 'Resume-MoveRequest' -ExchangeOrganization $ExchangeOrganization -splat $RMRParams
            Write-Log -Message $logstring -Verbose -EntryType Succeeded
        }
        Catch
        {
          Write-Log -verbose -errorlog -Message $logstring -EntryType Failed
          Write-Log -Message $_.tostring() -ErrorLog
        } 
    }#foreach
}#if
else
{
    Write-Log -verbose -errorlog -Message "ERROR: Unable to Proceed with Move Request Delta Synchronizations for $wave because Convergence Check FAILED" 
}#else
}#function Resume-MRMMoveRequestForDeltaSync
function Suspend-MRMMoveRequest
{
[cmdletbinding()]
param
(
    [parameter(Mandatory)]
    [string]$wave
    ,
    [parameter(Mandatory)]
    [ValidateSet('Full','Sub')]
    [string]$wavetype
    ,
    [parameter(Mandatory)]
    [string]$ExchangeOrganization #convert to dynamic parameter
    ,
    [parameter(Mandatory)]
    $SourceData
    ,
    [switch]$ByPassConvergenceCheck
    ,
    [switch]$IncludeBadADLookkupStatusInConvergenceCheck
    ,
    [parameter()]
    [string]$SuspendComment
)
switch ($wavetype)
{
        'Full' {$WaveSourceData = $SourceData | Where-Object {$_.Wave -match "\b$wave(\.\S*|\b)"}}
        'Sub' {$WaveSourceData = $SourceData | Where-Object {$_.wave -eq $wave}}
}
#check for convergence of Move Requests and Wave Tracking
if ($ByPassConvergenceCheck)
{
    Write-Log -Message "WARNING:Migration Tracking Database and Move Request Convergence for $Wave has been bypassed." -EntryType Notification -Verbose
    $proceed = $true
}
else {
    $TestConvergenceParams =
    @{
        Wave = $wave
        WaveType = $wavetype
        ExchangeOrganization = $ExchangeOrganization
        IncludeBadADLookupStatus = $IncludeBadADLookkupStatusInConvergenceCheck
        SourceData = $SourceData
        Report = 'All'
    }
    $proceed = Test-MRMConvergence @TestConvergenceParams
}
#If Convergence checks out or Bypass Convergence was chosen, proceed with move request delta synchronizations.
if ($proceed -eq $true)
{
    $b = 0
    $RecordCount = $WaveSourceData.count
    $SMRParams = @{
      ErrorAction = 'Stop'
      Confirm = $false
    }
    if (-not [string]::IsNullOrWhiteSpace($SuspendComment))
    {
      $SMRParams.SuspendComment = $SuspendComment
    }
    foreach ($request in $WaveSourceData)
    {
        $b++
        Write-Progress -Activity "Processing move request suspend for all $wave move requests." -Status "Processing $($Request.UserPrincipalName), record $b of $RecordCount." -PercentComplete ($b/$RecordCount*100)
        Try
        {
            Connect-Exchange -ExchangeOrganization $ExchangeOrganization > $null
            $logstring = "Suspend Move Request $($Request.UserPrincipalName) with Exchange GUID $($request.ExchangeGuid) for Delta Sync."
            Write-Log -Message $logstring -Verbose -EntryType Attempting
            $SMRParams.Identity = $request.ExchangeGuid
            Invoke-ExchangeCommand -cmdlet 'Suspend-MoveRequest' -ExchangeOrganization $ExchangeOrganization -splat $SMRParams
            Write-Log -Message $logstring -Verbose -EntryType Succeeded
        }
        Catch
        {
          Write-Log -verbose -errorlog -Message $logstring -EntryType Failed
          Write-Log -Message $_.tostring() -ErrorLog
        } 
    }#foreach
}#if
else
{
    Write-Log -verbose -errorlog -Message "ERROR: Unable to Proceed with Move Request Suspensions for $wave because Convergence Check FAILED" 
}#else
}#function Suspend-MRMMoveRequest
function Get-MRMMoveRequestReport
{
[cmdletbinding()]
param
(
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
Begin
{
    function Get-MoveRequestForWave {
        switch ($WaveType) {
            'Full' {
                Connect-Exchange -ExchangeOrganization $ExchangeOrganization > $null
                $Logstring = "Get all existing wave $wave move requests"
                Write-Log -message $Logstring -Verbose -EntryType Attempting 
                $Script:mr = @(Invoke-ExchangeCommand -cmdlet Get-MoveRequest -string "-BatchName $Wave* -ResultSize Unlimited" -ExchangeOrganization $ExchangeOrganization | where-object {$_.batchname -match "\b$wave(\.\S*|\b)"})
                #add error handling
            }
            'Sub' {
                Connect-Exchange -ExchangeOrganization $ExchangeOrganization > $null
                $Logstring = "Get all existing sub wave $wave move requests"
                Write-Log -message $Logstring -Verbose -EntryType Attempting 
                $Script:mr = @(Invoke-ExchangeCommand -cmdlet Get-MoveRequest -string "-BatchName $Wave -ResultSize Unlimited" -ExchangeOrganization $ExchangeOrganization)
                #add error handling
            }
        }
        $Script:fmr = @($mr | ? {$_.status -eq 'Failed'})
        $Script:ipmr = @($mr | ? {$_.status -eq 'InProgress'})
        $Script:asmr = @($mr | ? {$_.status -eq 'AutoSuspended'})
        $Script:cmr = @($mr | ? {$_.status -like 'Completed*'})
        $Script:qmr = @($mr | ? {$_.status -eq 'Queued'})
    }
}
Process
{
    switch ($operation) {
        'LargeItemReport' {
            Get-MoveRequestForWave
            $logstring = "Getting Statistics for all failed $wave move requests." 
            Write-Log -Message $logstring -EntryType Attempting -Verbose
            $RecordCount=$Script:fmr.count
            $b=0
            $Script:fmrs = @()
            foreach ($request in $fmr) {
                $b++
                Write-Progress -Activity $logstring -Status "Processing Record $b of $RecordCount." -PercentComplete ($b/$RecordCount*100)
                Connect-Exchange -ExchangeOrganization $ExchangeOrganization > $null
                $Script:fmrs += Invoke-ExchangeCommand -cmdlet Get-MoveRequestStatistics -string "Identity $($request.exchangeguid)"
            }
            if ($failedsince) {
                $logstring =  "Getting Statistics for all large item failed $wave move requests, failed since $FailedSince."
                Write-Log -Message $logstring -EntryType Attempting -Verbose
                $slifmrs = @($Script:fmrs | ? {$_.FailureType -eq 'TooManyLargeItemsPermanentException' -and $_.FailureTimeStamp -gt $FailedSince})
                }                        
            else {
                $logstring =  "Getting Statistics for all large item failed $wave move requests."
                Write-Log -Message $logstring -EntryType Attempting -Verbose
                $slifmrs = @($Script:fmrs | ? {$_.FailureType -eq 'TooManyLargeItemsPermanentException'})
                }
            $RecordCount=$slifmrs.count
            $b=0
            $Script:lifmrs = @()
            foreach ($request in $slifmrs) {
                $b++
                Write-Progress -Activity "Getting move request statistics for all large item failed $wave move requests." -Status "Processing Record $b  of $RecordCount." -PercentComplete ($b/$RecordCount*100)
                Connect-Exchange -ExchangeOrganization $ExchangeOrganization > $null
                $Script:lifmrs += $request | ForEach-Object {Invoke-ExchangeCommand -cmdlet Get-MoveRequestStatistics -string "-Identity $($_.alias) -IncludeReport" -ExchangeOrganization $ExchangeOrganization} | Select-Object -Property Alias,AllowLargeItems,ArchiveDomain,ArchiveGuid,BadItemLimit,BadItemsEncountered,BatchName,BytesTransferred,BytesTransferredPerMinute,CompleteAfter,CompletedRequestAgeLimit,CompletionTimestamp,DiagnosticInfo,Direction,DisplayName,DistinguishedName,DoNotPreserveMailboxSignature,ExchangeGuid,FailureCode,FailureSide,FailureTimestamp,FailureType,FinalSyncTimestamp,Flags,Identity,IgnoreRuleLimitErrors,InitialSeedingCompletedTimestamp,InternalFlags,IsOffline,IsValid,ItemsTransferred,LargeItemLimit,LargeItemsEncountered,LastUpdateTimestamp,MailboxIdentity,Message,MRSServerName,OverallDuration,PercentComplete,PositionInQueue,Priority,Protect,QueuedTimestamp,RecipientTypeDetails,RemoteArchiveDatabaseGuid,RemoteArchiveDatabaseName,RemoteCredentialUsername,RemoteDatabaseGuid,RemoteDatabaseName,RemoteGlobalCatalog,RemoteHostName,SourceArchiveDatabase,SourceArchiveServer,SourceArchiveVersion,SourceDatabase,SourceServer,SourceVersion,StartAfter,StartTimestamp,Status,StatusDetail,Suspend,SuspendedTimestamp,SuspendWhenReadyToComplete,SyncStage,TargetArchiveDatabase,TargetArchiveServer,TargetArchiveVersion,TargetDatabase,TargetDeliveryDomain,TargetServer,TargetVersion,TotalArchiveItemCount,TotalArchiveSize,TotalDataReplicationWaitDuration,TotalFailedDuration,TotalFinalizationDuration,TotalIdleDuration,TotalInProgressDuration,TotalMailboxItemCount,TotalMailboxSize,TotalProxyBackoffDuration,TotalQueuedDuration,TotalStalledDueToCIDuration,TotalStalledDueToHADuration,TotalStalledDueToMailboxLockedDuration,TotalStalledDueToReadCpu,TotalStalledDueToReadThrottle,TotalStalledDueToReadUnknown,TotalStalledDueToWriteCpu,TotalStalledDueToWriteThrottle,TotalStalledDueToWriteUnknown,TotalSuspendedDuration,TotalTransientFailureDuration,ValidationMessage,WorkloadType,@{n="BadItemList";e={@($_.Report.BadItems)}},@{n="LargeItemList";e={@($_.Report.LargeItems)}}
            }
            $logstring = "Getting Statistics for all communication error failed $wave move requests."
            Write-Log -Message $logstring -EntryType Attempting 
            $scefmrs = @($Script:fmrs | ? {$_.FailureType -eq 'CommunicationErrorTransientException'})
            $RecordCount=$scefmrs.count
            $b=0
            $Script:cefmrs = @()
            foreach ($request in $scefmrs) {
                $b++
                Write-Progress -Activity $logstring -Status "Processing Record $b of $RecordCount." -PercentComplete ($b/$RecordCount*100)
                Connect-Exchange -ExchangeOrganization $ExchangeOrganization > $null
                $Script:cefmrs += $request | ForEach-Object {Invoke-ExchangeCommand -cmdlet Get-MoveRequestStatistics -string "-Identity $($_.alias) -IncludeReport" -ExchangeOrganization $ExchangeOrganization} | Select-Object -Property Alias,AllowLargeItems,ArchiveDomain,ArchiveGuid,BadItemLimit,BadItemsEncountered,BatchName,BytesTransferred,BytesTransferredPerMinute,CompleteAfter,CompletedRequestAgeLimit,CompletionTimestamp,DiagnosticInfo,Direction,DisplayName,DistinguishedName,DoNotPreserveMailboxSignature,ExchangeGuid,FailureCode,FailureSide,FailureTimestamp,FailureType,FinalSyncTimestamp,Flags,Identity,IgnoreRuleLimitErrors,InitialSeedingCompletedTimestamp,InternalFlags,IsOffline,IsValid,ItemsTransferred,LargeItemLimit,LargeItemsEncountered,LastUpdateTimestamp,MailboxIdentity,Message,MRSServerName,OverallDuration,PercentComplete,PositionInQueue,Priority,Protect,QueuedTimestamp,RecipientTypeDetails,RemoteArchiveDatabaseGuid,RemoteArchiveDatabaseName,RemoteCredentialUsername,RemoteDatabaseGuid,RemoteDatabaseName,RemoteGlobalCatalog,RemoteHostName,SourceArchiveDatabase,SourceArchiveServer,SourceArchiveVersion,SourceDatabase,SourceServer,SourceVersion,StartAfter,StartTimestamp,Status,StatusDetail,Suspend,SuspendedTimestamp,SuspendWhenReadyToComplete,SyncStage,TargetArchiveDatabase,TargetArchiveServer,TargetArchiveVersion,TargetDatabase,TargetDeliveryDomain,TargetServer,TargetVersion,TotalArchiveItemCount,TotalArchiveSize,TotalDataReplicationWaitDuration,TotalFailedDuration,TotalFinalizationDuration,TotalIdleDuration,TotalInProgressDuration,TotalMailboxItemCount,TotalMailboxSize,TotalProxyBackoffDuration,TotalQueuedDuration,TotalStalledDueToCIDuration,TotalStalledDueToHADuration,TotalStalledDueToMailboxLockedDuration,TotalStalledDueToReadCpu,TotalStalledDueToReadThrottle,TotalStalledDueToReadUnknown,TotalStalledDueToWriteCpu,TotalStalledDueToWriteThrottle,TotalStalledDueToWriteUnknown,TotalSuspendedDuration,TotalTransientFailureDuration,ValidationMessage,WorkloadType,@{n="TotalTransientFailureMinutes";e={@($_.TotalTransientFailureDuration.TotalMinutes)}},@{n="TotalStalledDueToMailboxLockedMinutes";e={@($_.TotalStalledDueToMailboxLockedDuration.TotalMinutes)}}
           }
        }
        'UpdateMigrationWaveTracking' {
            $logstring = "Getting all available move requests for Migration Wave Tracking Update"
            Write-Log -Message $logstring -EntryType Attempting
            Connect-Exchange -ExchangeOrganization $ExchangeOrganization > $null
            $Script:mr = Invoke-ExchangeCommand -cmdlet Get-MoveRequest -string "-ResultSize Unlimited" -ExchangeOrganization $ExchangeOrganization
            $Script:fmr = @($mr | ? {$_.status -eq 'Failed'})
            $Script:ipmr = @($mr | ? {$_.status -eq 'InProgress'})
            $Script:asmr = @($mr | ? {$_.status -eq 'AutoSuspended'})
            $Script:cmr = @($mr | ? {$_.status -like 'Completed*'})
            $Script:qmr = @($mr | ? {$_.status -eq 'Queued'})
        }
        'WaveMonitoring' {
            Get-MoveRequestForWave
        }
        'Offboarding' {
            $logstring = "Getting all available offboarding move requests"
            Write-Log -Message $logstring -EntryType Attempting
            Connect-Exchange -ExchangeOrganization $ExchangeOrganization > $null
            $Script:mr = Invoke-ExchangeCommand -cmdlet Get-MoveRequest -string "-ResultSize Unlimited" | where-object {$_.direction -eq 'Push'}
            $Script:fmr = @($mr | ? {$_.status -eq 'Failed'})
            $Script:ipmr = @($mr | ? {$_.status -eq 'InProgress'})
            $Script:asmr = @($mr | ? {$_.status -eq 'AutoSuspended'})
            $Script:cmr = @($mr | ? {$_.status -like 'Completed*'})
            $Script:qmr = @($mr | ? {$_.status -eq 'Queued'})     
        }
    }
    switch ($statsoperation) {
        'All' {
            $logstring = "Getting move request statistics for all $wave move requests." 
            Write-Log -Message $logstring -EntryType Attempting 
            $RecordCount=$Script:mr.count
            $b=0
            $Script:mrs = @()
            foreach ($request in $Script:mr) {
                $b++
                Write-Progress -Activity $logstring -Status "Processing Record $b of $RecordCount." -PercentComplete ($b/$RecordCount*100)
                Connect-Exchange -ExchangeOrganization $ExchangeOrganization > $null
                $Script:mrs += Invoke-ExchangeCommand -cmdlet Get-MoveRequestStatistics -string "-Identity $($request.exchangeguid)" -ExchangeOrganization $ExchangeOrganization
            }
            $Script:ipmrs = @($Script:mrs | where-object {$psitem.status -like 'InProgress'})
            $Script:fmrs = @($Script:mrs | where-object {$psitem.status -like 'Failed'})
            $Script:cmrs = @($Script:mrs |  where-object {$psitem.status -like 'Completed*'})
        }
        'Failed' {
            $logstring = "Getting Statistics for all failed $wave move requests."
            Write-Log -Message $logstring -EntryType Attempting
            $RecordCount=$Script:fmr.Count
            $b=0
            $Script:fmrs = @()
            foreach ($request in $fmr) {
                $b++
                Write-Progress -Activity $logstring -Status "Processing Record $b of $RecordCount." -PercentComplete ($b/$RecordCount*100)
                Connect-Exchange -ExchangeOrganization $ExchangeOrganization > $null
                $Script:fmrs += Invoke-ExchangeCommand -cmdlet Get-MoveRequestStatistics -string "-Identity $($request.exchangeguid)" -ExchangeOrganization $ExchangeOrganization
            }
        }
        'InProgress' {
            $logstring = "Getting Statistics for all in progress $wave move requests."
            Write-Log -Message $logstring -EntryType Attempting
            $RecordCount=$Script:ipmr.Count
            $b=0
            $Script:ipmrs = @()
            foreach ($request in $ipmr) {
                $b++
                Write-Progress -Activity $logstring -Status "Processing Record $b of $RecordCount." -PercentComplete ($b/$RecordCount*100)
                Connect-Exchange -ExchangeOrganization $ExchangeOrganization > $null
                $Script:ipmrs += Invoke-ExchangeCommand -cmdlet Get-MoveRequestStatistics -string "-Identity $($request.exchangeguid)" -ExchangeOrganization $ExchangeOrganization
            }
        }
    }
}
End
{
    if ($passthru) {
        $Script:mr 
    }
}
}#function Get-MRMMoveRequestReport
function Watch-MRMMoveRequest
{
[cmdletbinding()]
param
(
    [parameter()]
    [ValidateSet('Completion','Synchronization')]
    [string]$Operation
    ,
    [parameter(Mandatory=$true)]
    [validateSet('Full','Sub')]
    [string]$wavetype = 'Sub'
    ,
    [parameter(Mandatory=$true)]
    [string]$wave
    ,
    [string[]]$Recipients
    ,
    [string]$Sender
    ,
    [string]$ExchangeOrganization
    #,
    #$SourceData = $Script:sourcedata
    #add convergence check into report data based on Source Data
)
#check for the wave completion hash table and create it if it does not exist
if (-not (Test-Path 'variable:\WaveMigrationOperationCompleted')) {$script:WaveMigrationOperationCompleted = @{}}
#check for the wave completion entry in the wave completion hash table and create it if it does not exist
if (-not ($script:WaveMigrationOperationCompleted.ContainsKey($wave))) {$script:WaveMigrationOperationCompleted.$wave = $False}
#if the wave completion entry in the wave completion hash table indicates the wave is not complete, run the move request report
if ($script:WaveMigrationOperationCompleted.$wave -eq $false)
{
[string]$Stamp = Get-TimeStamp
#switch ($wavetype) 
#{
#    'Full' {$WaveSourceData = $SourceData | Where-Object {$_.Wave -match "\b$wave(\.\S*|\b)"}}
#    'Sub' {$WaveSourceData = $SourceData | Where-Object {$_.wave -eq $wave}}
#}
$message = "Get Migration Wave $wave Move Request Report."
Write-Log -message $message -Verbose -EntryType Attempting
Get-MRMMoveRequestReport -wave $wave -WaveType $wavetype -operation WaveMonitoring -statsoperation All -ExchangeOrganization $ExchangeOrganization -ErrorAction Stop
Write-Log -message $message -Verbose -EntryType Succeeded 
#check if the Wave Migration Monitoring Hash Table exists; if not, create it.  
if (-not (Test-Path 'variable:\WaveMigrationMonitoring')) {$Script:WaveMigrationMonitoring = @{}}
#check the Wave Migration Operation Status
switch ($Operation)
{
    'Completion'
    {
        if ($Script:ipmrs.count -ge 1 -or $Script:qmr.Count -ge 1 -or $Script:asmr.Count -ge 1)
        {
            if ($Script:fmr.Count -ge 1)
            {
                $Script:WaveMigrationMonitoring.$wave = 'CompletionInProgressWithFailures'
            }
            else
            {
                $Script:WaveMigrationMonitoring.$wave = 'CompletionInProgress'
            }
            $script:WaveMigrationOperationCompleted.$wave = $false
        }
        else
        {
            if ($Script:fmr.Count -ge 1)
            {
                $Script:WaveMigrationMonitoring.$wave = 'CompletedWithFailures'
            }
            else
            {
                $Script:WaveMigrationMonitoring.$wave = 'Completed'
            }
        }
    }
    'Synchronization'
    {
        if ($Script:ipmrs.count -ge 1 -or $Script:qmr.Count -ge 1)
        {
            if ($Script:fmr.Count -ge 1)
            {
                $Script:WaveMigrationMonitoring.$wave = 'SynchronizationInProgressWithFailures'
            }
            else
            {
                $Script:WaveMigrationMonitoring.$wave = 'SynchronizationInProgress'
            }
            $script:WaveMigrationOperationCompleted.$wave = $false
        }
        else
        {
            if ($Script:fmr.Count -ge 1)
            {
                $Script:WaveMigrationMonitoring.$wave = 'SynchronizedWithFailures'
            }
            else
            {
                $Script:WaveMigrationMonitoring.$wave = 'Synchronized'
            }
        }
    }
}
#if the wave operation is still in progress, send mail notification, otherwise, send mail notification 1 time for completed operation but not again.
if ($Script:WaveMigrationMonitoring.$wave -like '*InProgress*')
{
    $MailNotification = $true
}
elseif ($script:WaveMigrationOperationCompleted.$wave -eq $false)
{
    $MailNotification = $true
    $script:WaveMigrationOperationCompleted.$wave =  $true
}
else 
{
    $MailNotification = $false
}
#Send the Mail Notification
if ($mailNotification -and $Script:mr.count -gt 0)
{
    #http://stackoverflow.com/questions/11035905/powershell-display-current-time-with-time-zone
    $TimeZone = [Regex]::Replace([System.TimeZoneInfo]::Local.StandardName, '([A-Z])\w+\s*', '$1')
    [string]$MessageTimeStamp = (Get-Date -Format 'yyyy-MM-dd HH:mm') + " $TimeZone"
    $sendmailparams = @{}
    $sendmailparams.Subject = "Auto Generated Message: Wave $wave Mailbox Move $Operation Status Report Update as of $MessageTimeStamp"
    #below needs to go in admin user profile or org profile
	$Sendmailparams.From = $Sender
    $Sendmailparams.To = $Recipients
    $Sendmailparams.SmtpServer = (Get-OneShellVariableValue -Name CurrentOrgProfile).general.mailrelayserverFQDN
    $sendmailparams.BodyAsHtml = $true
    $sendmailparams.Attachments = ($ExportDataPath + 'AllStatus.csv')
    #mail contents
    #create the All Status attachment
    $Script:mrs | Select-Object MailboxIdentity,DisplayName,Alias,@{n='Wave';e={$_.Batchname}},Status,StatusDetail,PercentComplete,CompletionTimestamp | Sort-Object DisplayName | Export-Csv -NoTypeInformation -Force -Path ($ExportDataPath + 'AllStatus.csv')
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
#region CreateHTMLContentTables
    #Create the html content tables with the css applied    
    $IPR = $Script:ipmrs | Select-Object DisplayName,Alias,BatchName,PercentComplete,TotalMailboxSize,TotalMailboxItemCount,ItemsTransferred,Status,StatusDetail,RemoteHostName | Sort-Object PercentComplete | ConvertTo-Html -As Table -Head $css
    $IPSR = $Script:ipmrs | Select Status,StatusDetail | Group-Object StatusDetail | sort Name | Select @{n='Status Detail';e={$_.Name}},Count| ConvertTo-Html -as Table -Head $css
    if ($Script:fmrs.count -ge 1)  {$FR = $Script:fmrs | Select-Object DisplayName,Alias,BatchName,Status,StatusDetail,FailureType,FailureSide,FailureTimestamp | Sort-Object DisplayName | ConvertTo-Html -as Table -Head $css}
    $CR = $Script:cmrs | Select-Object DisplayName,Alias,BatchName,PercentComplete,Status,StartTimeStamp,CompletionTimestamp | Sort-Object DisplayName | ConvertTo-Html -as Table -Head $css
if ($wavetype -eq 'Full') {
    $IPSRwS= $Script:ipmrs | select Status,StatusDetail,BatchName | Group-Object BatchName,StatusDetail | sort Name | Select @{n='Sub Wave, Status Detail';e={$_.Name}},Count | ConvertTo-Html -As Table-Head $css
    $TMRwS = $Script:mr | Group-Object BatchName | sort Name | Select @{n='Sub Wave';e={$_.Name}},Count | ConvertTo-Html -As Table -Head $css
    $TMRSwS = $Script:mr | Group-Object BatchName,Status | sort Name | Select @{n='Sub Wave, Status';e={$_.Name}},Count| ConvertTo-Html -As Table -Head $css
}
#endregion CreateHTMLContentTables
#region CreateHTMLMessageBody
    $Body = 
@"
<b>Wave $wave Mailbox Move $Operation Status Report.</b><br><br> 
<b>Current Wave $wave $Operation Status: $($Script:WaveMigrationMonitoring.$wave)</b><br><br>
Immediately following is summary information, followed by more detail per mailbox move. <br>
Attached in csv file format is status for each wave $wave mailbox move, current as of the generation of this message. <br><br> 
<b>Status summary for all $wave mailbox moves:</b><br>
Total Moves:`t $($Script:mr.count)<br>
Completed:`t $($Script:cmr.count)<br>
In Progress:`t $($Script:ipmr.count)<br>
Queued:`t $($Script:qmr.count)<br>
AutoSuspended: `t $($Script:asmr.count)<br>
Failed: `t $($Script:fmr.count)<br><br>
<b>Status Detail Summary for all $wave In Progress mailbox moves:</b><br>
$IPSR
<br><br>
"@ 
#Add more body for Full Waves to break out sub-waves
if ($wavetype -eq 'Full') 
{
    $body +=
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
#Add more body for any failed move requests
if ($Script:fmrs.count -ge 1) 
{
    $body +=
@"
<b>Failure details for currently Failed wave $wave mailbox moves:</b><br>
$FR
<br><br>
"@ 
}
#Add final bit of body for in progress and completed details
$body += 
@"
<b>Statistics for currently In Progress wave $wave  mailbox moves:</b><br>
$IPR
<br><br>
<b>Completion details for all Completed wave $wave mailbox moves:</b><br>
$CR
<br><br>
"@ 
#endregion CreateHTMLMessageBody
    $Sendmailparams.Body = $Body
    Send-MailMessage @sendmailparams
    Write-Log -message "Monitoring E-mail Message Sent to recipients $($Recipients -join ';') " -Verbose 
}
}#if
}#function Watch-MRMMoveRequest
Function Watch-MRMMoveRequestContinuously
{
param
(
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
    [parameter()]
    [validateset('Completion','Synchronization')]
    [string]$Operation
    ,
    [switch]$CompletedMailboxConfiguration
    ,
    [switch]$resumeautosuspended
    ,
    [switch]$resumefailed
    ,
    [string[]]$Recipients
    ,
    [string]$Sender
    ,
    [string]$ExchangeOrganization #convert to Dynamic Parameter
    #,
    #$SourceData
)
while ($True)
{
    $time = get-date
    if ($time -ge $nextrun)
    {
        $lastrunstart = get-date
        $nextrun = $nextrun.AddMinutes($runperiod)
        Connect-Exchange -ExchangeOrganization $ExchangeOrganization > $null
        Write-Log "Running Watch-MRMMoveRequest" -Verbose
        $WMRParams =
        @{
            Operation = $Operation
            Wave = $Wave
            WaveType = $WaveType
		        ExchangeOrganization = $ExchangeOrganization
            Sender = $Sender
            Recipients = $Recipients
            #SourceData = $SourceData
        }
        Watch-MRMMoveRequest @WMRParams
        $lastruncompletion = get-date
        Write-Log "Last run of Watch-MoveRequest completed at $lastruncompletion." -Verbose -EntryType Notification
    }
    $timeremaining = $nextrun - $time
    $minutes = $timeremaining.Minutes
    $hours = $timeremaining.Hours
    if (($Minutes % 15) -eq 0 -or ($minutes -le 5 -and $hours -eq 0))
    {
        Write-Log "Next run of Watch-MoveRequest occurs in $minutes minutes at approximately $nextrun" -Verbose -EntryType Notification
    }
    #if ($resumeautosuspended) {#Run Resume AutoSuspended Code}
    #if ($resumefailed) {#Run Resume Failed Code}
    #if ($CompletedMailboxConfiguration) {#Run Mailbox Configuration Code}
    Start-Sleep -Seconds 60
}#while
}#function Watch-MRMMoveRequestContinuously
function Get-MRMNonDeletedLargeItemReport
{
[cmdletbinding()]
param
(
    [string]$Wave
    ,
    [datetime]$FailedSince
    ,
    [string]$ExchangeOrganization
    ,
    [Parameter(Mandatory)]
    [string]$outputCSVFileFullPathWithName
)
$LIReports = @()
#hash table for parameters for Get-MoveRequestReportData
$GetMRRD = @{}
$GetMRRD.Wave = $Wave
$GetMRRD.WaveType = 'Full'
$GetMRRD.Operation = 'LargeItemReport'
$GetMRRD.ExchangeOrganization = $ExchangeOrganization
if ($failedsince) {$GetMRRD.FailedSince = $FailedSince}
Get-MRMMoveRequestReport @GetMRRD
foreach ($request in $Script:lifmrs)
{
    $DisplayName = $($request.MailboxIdentity.Rdn.UnescapedName)
    $FailureTimeStamp = $($request.FailureTimeStamp)
    $QualifiedLargeItems = @(
        $request.LargeItemList | ? {$_.WellKnownFolderType.tostring() -ne 'DumpsterDeletions'} | 
        foreach-object {"Subject: $($_.Subject); Folder: $($_.FolderName); Date: $($_.DateReceived); Sender: $($_.Sender); Recipient: $($_.Recipient); Size: $($_.MessageSize/1MB -as [int])MB"}
    )
    $LItemsNotDeletedList = $QualifiedLargeItems -join "`r`n"
    $QualifiedLargeItemCount = $QualifiedLargeItems.count
    If ($QualifiedLargeItemCount -gt 0) {
        Connect-Exchange -ExchangeOrganization $ExchangeOrganization > $null
        $OLRecipientPrimarySmtpAddress = Get-OLRecipient -Identity $DisplayName | Select-Object -ExpandProperty PrimarySmtpAddress
        $LIReport = New-Object -TypeName PSObject -Property @{DisplayName = $DisplayName; PrimarySmtpAddress = $OLRecipientPrimarySmtpAddress; LargeOrBadItemCount = $QualifiedLargeItemCount; LargeOrBadItemList = $LItemsNotDeletedList; FailureTimeStamp = $FailureTimeStamp}
        $LIReports += $LIReport | Select-Object DisplayName,PrimarySmtpAddress,LargeOrBadItemCount,LargeOrBadItemList,FailureTimeStamp
    }
}#foreach
if ($LIReports.count -gt 0)
{
    $LIReports | Export-Csv -NoTypeInformation -Path $outputCSVFileFullPathWithName -Append
}#If $LIReports.count -gt 0
}#function Get-MRMNonDeletedLargeItemReport
#New/Experimental:
function Start-MRMBackgroundWMRC 
{
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
###################################################################################################
#pre/post migration configuration functions - Under Development
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
        [string]$logpath = $logfolderpath + $Stamp + '-' + $UserPrincipalName + '-ConfigureMailboxOptions.log'
        [string]$errorlogpath = $logfolderpath + $Stamp + '-' + $UserPrincipalName + '-ERRORS-ConfigureMailboxOptions.log'
        [string]$completionsfile = $exportdataPath + $wave + '-MailboxConfigurationCompletionTracking.csv'
        $waveSourceData = @($SourceData | ? UserPrincipalName -eq $UserPrincipalName)
    }
    'MigrationWave' {
        [string]$logpath = $logfolderpath + $Stamp + '-' + $Wave + '-ConfigureMailboxOptions.log'
        [string]$errorlogpath = $logfolderpath + $Stamp + '-' + $Wave + '-ERRORS-ConfigureMailboxOptions.log'
        [string]$completionsfile = $exportdataPath + $wave + '-MailboxConfigurationCompletionTracking.csv'
        switch ($wavetype) {
        'Full' {$WaveSourceData = @($SourceData | Where-Object {$_.Wave -match "\b$wave(\.\S*|\b)"})}
        'Sub' {$WaveSourceData = @($SourceData | Where-Object {$_.wave -eq $wave})}
        }
    }
}

if (-not $Script:ForwardingConfigurations) {
    Write-Log "Identifying most recent Forwarding Configurations File in Source Data Folder $ReferenceFolder"
    Try {
        $ForwardingConfigurationsFile = Get-ChildItem -Path $ReferenceFolder -Filter *ForwardingConfigurations.csv -ErrorAction stop | Sort-Object -Property LastWriteTime -Descending | Select-Object -First 1
        if ($ForwardingConfigurationsFile) {
            Write-Log -message "Most recent Forwarding Configurations File $($ForwardingConfigurationsFile.FullName) identified in Source Data Folder $ReferenceFolder" -Verbose 
            $Script:ForwardingConfigurations = Import-Csv $ForwardingConfigurationsFile.FullName -ErrorAction Stop}
        }
    Catch {
        Write-Log -message "ERROR: Unable to identify the most recent Forwarding Configurations File in Source Data Folder $ReferenceFolder" -ErrorLog
        $_
    }
}
if (-not $Script:SendAsConfigurations) {
    Write-Log "Identifying most recent Send As Configurations File in Source Data Folder $ReferenceFolder"
    Try {
        $SendAsConfigurationsFile = Get-ChildItem -Path $ReferenceFolder -Filter *SendAsConfigurations.csv -ErrorAction stop | Sort-Object -Property LastWriteTime -Descending | Select-Object -First 1
        if ($SendAsConfigurationsFile) {
            Write-Log -message "Most recent Send As Configurations File $($SendAsConfigurationsFile.FullName) identified in Source Data Folder $ReferenceFolder" -Verbose 
            $Script:SendAsConfigurations = Import-Csv $SendAsConfigurationsFile.FullName -ErrorAction Stop
        }
        }
    Catch {
        Write-Log -message "ERROR: Unable to identify the most recent Send As Configurations File in Source Data Folder $ReferenceFolder" -ErrorLog
        $_
    }
}
if (-not $Script:FullAccessConfigurations) {
    Write-Log "Identifying most recent Full Access Configurations File in Source Data Folder $ReferenceFolder"
    Try {
        $FullAccessConfigurationsFile = Get-ChildItem -Path $ReferenceFolder -Filter *FullAccessConfigurations.csv -ErrorAction stop | Sort-Object -Property LastWriteTime -Descending | Select-Object -First 1
        if ($FullAccessConfigurationsFile) {
            Write-Log -message "Most recent Full Access Configurations File $($FullAccessConfigurationsFile.FullName) identified in Source Data Folder $ReferenceFolder" -Verbose 
            $Script:FullAccessConfigurations = Import-Csv $FullAccessConfigurationsFile.FullName -ErrorAction Stop
        }
        }
    Catch {
        Write-Log -message "ERROR: Unable to identify the most recent Full Access Configurations File in Source Data Folder $ReferenceFolder" -ErrorLog
        $_
    }
}
Switch ($operation) {
    'ExchangePostMigration' {
        Write-Log -message "Beginning Configuration Operations for Completed Mailbox Moves" -Verbose -LogPath $LogPath       
        #record completed moves into input file 
        $completions = @($Script:cmr | Select-Object DisplayName,DistinguishedName,ExchangeGuid,RecipientType,RecipientTypeDetails,Status)
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
                Connect-Exchange -ExchangeOrganization $exchangeOrganization > $null
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
                Connect-Exchange -ExchangeOrganization $exchangeOrganization > $null
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
    Write-Log -Message "Attempting Export of Failed Mailbox Configurations to Export Data Folder $ExportDataPath." -Verbose -LogPath $LogPath
    Export-Data -DataToExportTitle MailboxConfigurationFailures -DataToExport $failedconfigurations -datatype csv 
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
if (-not $Script:FullAccessConfigurations) {
    Write-Log "Identifying most recent Full Access Configurations File in Source Data Folder $ReferenceFolder"
    Try {
        $FullAccessConfigurationsFile = Get-ChildItem -Path $ReferenceFolder -Filter *FullAccessConfigurations.csv -ErrorAction stop | Sort-Object -Property LastWriteTime -Descending | Select-Object -First 1
        Write-Log -message "Most recent Full Access Configurations File $($FullAccessConfigurationsFile.FullName) identified in Source Data Folder $ReferenceFolder" -Verbose 
        $Script:FullAccessConfigurations = Import-Csv $FullAccessConfigurationsFile.FullName -ErrorAction Stop
        }
    Catch {
        Write-Log -message "ERROR: Unable to identify the most recent Full Access Configurations File in Source Data Folder $ReferenceFolder" -ErrorLog
        $_
    }
}
if ($SingleMailbox) {
    $FullaccessPerms = @($Script:FullAccessConfigurations | ? IdentityPrimarySmtpAddress -eq $IdentityPrimarySmtpAddress)
}
else {$FullaccessPerms = $Script:FullAccessConfigurations}
$RecordCount = $FullaccessPerms.Count
$b=0
if ($RecordCount -gt 0) {
    foreach ($perm in $FullaccessPerms) {
        Connect-Exchange -ExchangeOrganization $exchangeOrganization > $null
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
    if (-not $Script:ForwardingConfigurations) {
        Write-Log "Identifying most recent Forwarding Configurations File in Source Data Folder $ReferenceFolder"
        Try {
            $ForwardingConfigurationsFile = Get-ChildItem -Path $ReferenceFolder -Filter *ForwardingConfigurations.csv -ErrorAction stop | Sort-Object -Property LastWriteTime -Descending | Select-Object -First 1
            Write-Log -message "Most recent Forwarding Configurations File $($ForwardingConfigurationsFile.FullName) identified in Source Data Folder $ReferenceFolder" -Verbose 
            If ($ForwardingConfigurationsFile) {$Script:ForwardingConfigurations = Import-Csv $ForwardingConfigurationsFile.FullName -ErrorAction Stop}
            }
        Catch {
            Write-Log -message "ERROR: Unable to identify the most recent Forwarding Configurations File in Source Data Folder $ReferenceFolder" -ErrorLog
            $_
        }
    }
    if (-not $Script:ForwardingConfigurationsIdentities) {
       $Script:ForwardingConfigurationsIdentities = $Script:ForwardingConfigurations | select -ExpandProperty Identity
    }

    IF ($IdentityPrimarySmtpAddress -in $Script:ForwardingConfigurationsIdentities) {
        try {
            $forwardingconfig = $Script:ForwardingConfigurations | ? Identity -eq $IdentityPrimarySmtpAddress
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