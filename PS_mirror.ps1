
$StartDate=(GET-DATE)

. .\local_env_variables.ps1

### Set procedure variables
$server_name = $Env:sqlservername
$Username = $Env:sql3user
$Password = $Env:sql3password
$clientidsecret = $Env:PS_CLIENT_SECRET
$headers = @{"Authorization"="Bearer "+$api_key}

#region Setup/configuration information

#### setup array for all tables to be mirrored. If adding a new table, need to just add it here.

#### Grab info from config table
$query_string="select config.value_a yearid from config where config_type='syncyear'"
$yearquery = Invoke-Sqlcmd -ServerInstance $server_name -database "canvas_currentyear" -query $query_string -Username $Username -Password $Password -ErrorAction Stop -QueryTimeout 600
$yearbody='{"YEARID":"'+$yearquery.yearid+'"}'

$query_string="select value_a termid from config where config_type='syncterms'"
$termquery = Invoke-Sqlcmd -ServerInstance $server_name -database "canvas_currentyear" -query $query_string -Username $Username -Password $Password -ErrorAction Stop -QueryTimeout 600
$termstring=$termquery.termid -join ","
$termbody = "{""termids"": [$termstring]}"

#endregion

#region Network communication/OAuth section

#################################################################################################
############ THIS SECTION OF CODE TEMPORARILY RELAXES useUnsafeHeaderParsing ####################
############ This was done because we were getting parsing errors on HTTP    ####################
############ response from PowerSchool REST call.                            ####################
#################################################################################################

$netAssembly = [Reflection.Assembly]::GetAssembly([System.Net.Configuration.SettingsSection])

if($netAssembly)
{
    $bindingFlags = [Reflection.BindingFlags] "Static,GetProperty,NonPublic"
    $settingsType = $netAssembly.GetType("System.Net.Configuration.SettingsSectionInternal")

    $instance = $settingsType.InvokeMember("Section", $bindingFlags, $null, $null, @())

    if($instance)
    {
        $bindingFlags = "NonPublic","Instance"
        $useUnsafeHeaderParsingField = $settingsType.GetField("useUnsafeHeaderParsing", $bindingFlags)

        if($useUnsafeHeaderParsingField)
        {
          $useUnsafeHeaderParsingField.SetValue($instance, $true)
        }
    }
}

########################################################################################
##### This piece of code sets the https Security Protocol level to TLS 1.2 #############
########################################################################################

[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

#################################################################################################
############################### Code really starts here. ########################################
#################################################################################################

######## Section to grab oauth access token ##########

$clientidsecret_bytes = [System.Text.Encoding]::UTF8.GetBytes($clientidsecret)
$api_key=[Convert]::ToBase64String($clientidsecret_bytes)

$headers = @{"Authorization"="Basic $api_key";"Content-Type"="application/x-www-form-urlencoded";"Accept"="application/json"}
$auth_req_body= "grant_type=client_credentials"

$api_url_prefix = "https://setoncatholic.powerschool.com/oauth/access_token/"
Import-Module SqlServer

$error.Clear()
try
{
    $results = (Invoke-WebRequest -Headers $headers -Body $auth_req_body -Method POST -Uri $api_url_prefix)
    $results = ConvertFrom-Json $results.Content
    $api_key = $results.access_token
}
catch
{
    Write-Host $error
    exit
}

############## Begin to code to mirror PS ###############

$error.Clear()

$headers = @{"Authorization"="Bearer $api_key";
                "Accept"="application/json; charset=utf-8";
                "Content-Type"="application/json; charset=utf-8"}
$db_name="PS_mirror"

### Had to add this line because the REST API calls were fail with error: The underlying connection was closed: An unexpected error occurred on a send
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

#endregion

#region REST call and db store section

$mirrortables = @(
    "terms"
    ,"u_def_ext_students"
    ,"CC"
    ,"person"
    ,"personemailaddresses"
    ,"personaddresses"
    ,"personphonenumbers"
    ,"studentcontactassoc"
    ,"studentcontactdetail"
    ,"teachers"
    ,"users"
    ,"students"
    ,"courses"
    ,"sections"
    ,"sectionteacher"
    ##,"pgfinalgrades"
    ##,"scheduleCC"
    ##,"schedulesections"
    ##,"assignments"
    ##,"assignment_scores"
    )

foreach ($table_name in $mirrortables)
{
    $error.Clear()
    try
    {
        $pgnm=1
        $lines=@()
        $lines += "TRUNCATE TABLE [dbo].[$table_name]"
        $lines_to_commit=0

        ### First page of 1000 records
        $api_url_prefix = "https://setoncatholic.powerschool.com/ws/schema/query/org.setoncatholic.$table_name" + "?page=$pgnm&pagesize=1000"
        Write-Host "Executing API pull from Powerschool for $table_name."

        do
        {
            Write-Host "Grabbing 1000 records for $table_name, page $pgnm."
            $api_url_prefix = "https://setoncatholic.powerschool.com/ws/schema/query/org.setoncatholic.$table_name" + "?page=$pgnm&pagesize=1000"
        
            if($table_name -eq "assignments") {$results = (Invoke-WebRequest -Headers $headers -Method POST -Body $termbody -Uri $api_url_prefix)}
            elseif($table_name -eq "assignment_scores")
            {
                $results = (Invoke-WebRequest -Headers $headers -Method POST -Body $termbody -Uri $api_url_prefix)
            }
            elseif($table_name -eq "CC")
            {
                $results = (Invoke-WebRequest -Headers $headers -Method POST -Body $yearbody -Uri $api_url_prefix)
            }
            else
            {
                $results = (Invoke-WebRequest -Headers $headers -Method POST -Uri $api_url_prefix)
            }
            $results_obj = ConvertFrom-Json($results.Content)

            ### increment number of records for periodic commit of records
            $lines_to_commit=$lines_to_commit+$results_obj.record.Count

            foreach ($rec in $results_obj.record)
            {
                $sqlrow_names = @()
                $sqlrow_values = @()
                foreach($prop in $rec.psobject.properties)
                {
                    if(![string]::IsNullOrEmpty($prop.Value))
                    {
                        $sqlrow_names += $prop.Name
                        $sqlrow_values += $prop.Value
                    }
                }
        
                ### join field names and field values for insert statement
                $sqlrow_names = $sqlrow_names -replace "'","''"
                $sqlrow_values = $sqlrow_values -replace "'","''"
                $name_string = $sqlrow_names -join "],["
                $value_string = $sqlrow_values -join "','"
                $lines += "INSERT INTO $table_name ([$name_string]) VALUES ('$value_string')"
            }

            ### Increment to bring next 1000/page if there are any records left
            ++$pgnm

            if(($lines_to_commit -ge 4000) -or ($results_obj.record.Count -lt 1000))
            {
                $query_string = $lines -join "`r`n" ### Concatenate all array elements together
                Write-Host "Executing SQL query to add rows to $table_name."

                $sql_commit = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query_string -Username $Username -Password $Password -ErrorAction Stop -Querytimeout 600
                $lines_to_commit=0
                $lines=@()
            }

        } while ($results_obj.record.Count -eq 1000)

        ### Reached end of try statement
        Write-Host "Table $table_name complete."

    }
    catch
    {
        Write-Host "Error while running query $table_name"
        $error
    }
}

#endregion

<#
#region MIRROR ALL ASSIGNMENTS SCORES
#########################################################################
#########################################################################

#### setup array for all tables to be mirrored. If adding a new table, need to just add it here.
$mirrortables = @("assignment_scores")

##$mirrortables = @("u_stu_contact") ##,"pgfinalgrades","scheduleCC")
$body = "{""termids"": ["+$termbody+"]}"
$lines_to_commit=0
$lines=@()

foreach ($table_name in $mirrortables)
{    
    $lines += "TRUNCATE TABLE [dbo].[$table_name]"
    $pgnm=1
    $ransuccesfully = $true
    $name_string="[course_number],[lastfirst],[assignment_name],[sectionid],[assignmentid],[percent],[studentid],[score],[exempt]"

    ### First page of 1000 records
    $api_url_prefix = "https://setoncatholic.powerschool.com/ws/schema/query/org.setoncatholic.$table_name" + "?page=$pgnm&pagesize=1000"
    Write-Host "Executing API pull from Powerschool for $table_name."

    do
    {
        Write-Host "Grabbing 1000 records for $table_name, page $pgnm."
        $api_url_prefix = "https://setoncatholic.powerschool.com/ws/schema/query/org.setoncatholic.$table_name" + "?page=$pgnm&pagesize=1000"
        try
        {
            $results = (Invoke-WebRequest -Headers $headers -Method POST -Body $termbody -Uri $api_url_prefix)
            $results_obj = ConvertFrom-Json($results.Content)
            $lines_to_commit=$lines_to_commit+$results_obj.record.Count
        }
        catch
        {
            Write-Host "Error while running query $table_name"
        }

        foreach ($rec in $results_obj.record) ## .tables.$table_name
        {
            ### [course_number],[lastfirst],[assignment_name],[sectionid],[assignmentid],[percent],[studentid],[score],[exempt]
            
            if($rec.score -eq "--") {$scrubbed_score="null"} else {$scrubbed_score="'"+[decimal]$rec.score+"'"}
            $scrubbed_lastfirst = $rec.lastfirst -replace "'", "''"
            $scrubbed_assignment_name = $rec.assignment_name -replace "'", "''"
            $value_string =
            "INSERT INTO $table_name ($name_string) VALUES (" +
                "'"+$rec.course_number+"',"+
                "'"+$scrubbed_lastfirst+"',"+
                "'"+$scrubbed_assignment_name+"',"+
                "'"+$rec.sectionid+"',"+
                "'"+$rec.assignmentid+"',"+
                "'"+$rec.percent+"',"+
                "'"+$rec.studentid+"',"+
                $scrubbed_score+","+
                "'"+$rec.exempt+"')"

            $lines += $value_string
        }

        ++$pgnm

        if(($lines_to_commit -ge 4000) -or ($results_obj.record.Count -lt 1000))
        {
            $query_string = $lines -join "`r`n" ### Concatenate all array elements together
            Write-Host "Executing SQL query to add rows to $table_name."

            try
            {
                $sql_commit = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query_string -Username $Username -Password $Password -ErrorAction Stop -Querytimeout 600
                $lines_to_commit=0
                $lines=@()
            }
            catch
            {
                Write-Host $error
                Exit
            }
        }
    }
    while ($results_obj.record.Count -eq 1000)
}

Write-Host "Assignments scores complete."
#>
#endregion


#region Logging section
######################################

$EndDate=(GET-DATE)
$timespan = NEW-TIMESPAN –Start $StartDate –End $EndDate
"Run time: " + $timespan.TotalMinutes + " minutes"

$errortostore = $Error -replace "'", "''"
$query = "insert into job_results values ('"+$EndDate+"','PS Mirror',"+$timespan.TotalMinutes+","+$error.Count+",'"+$errortostore+"')"
$results = (Invoke-Sqlcmd -ServerInstance $server_name -database "job_logs" -query $query -Username $Username -Password $Password -Querytimeout 600)


#endregion

