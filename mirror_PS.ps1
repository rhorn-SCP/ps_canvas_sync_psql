
$error.Clear()
$StartDate=(GET-DATE)
. .\local_env_variables.ps1
. .\sync_functions.ps1

#region Setup/configuration information

### Grab client secret for PS API/PowerQuery calls
$clientidsecret = $Env:PS_CLIENT_SECRET
$cnx_string = $Env:PSQL_CONNECTION_STRING

#### Grab sync configuration from local_env_variables
$yearbody='{"YEARID":'+$Env:syncyear+'}'
$termstring=($Env:ps_syncterms -split " ") -join ","
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

$ps_api_url = "https://setoncatholic.powerschool.com/oauth/access_token/"

try
{
    $results = (Invoke-WebRequest -Headers $headers -Body $auth_req_body -Method POST -Uri $ps_api_url)
    $results = ConvertFrom-Json $results.Content
    $api_key = $results.access_token
}
catch
{
    Write-Host $error
    exit
}

############## Begin to code to mirror PS ###############

$headers = @{"Authorization"="Bearer $api_key";
                "Accept"="application/json; charset=utf-8";
                "Content-Type"="application/json; charset=utf-8"}
$db_name="ps_mirror"

### Had to add this line because the REST API calls were fail with error: The underlying connection was closed: An unexpected error occurred on a send
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

#endregion

#region REST call and db store section

$mirrortables = $Env:ps_mirror_tables -split " "

foreach ($table_name in $mirrortables)
{
    try
    {
        $pgnm=1
        $query_string = 'TRUNCATE TABLE ps_mirror.' + $table_name + ';'
        $sql_commit = ExecuteNonQuery -ConnectionString $cnx_string -command_string $query_string
        $lines_to_commit=0

        ### First page of 1000 records
        $api_url_prefix = "https://setoncatholic.powerschool.com/ws/schema/query/org.setoncatholic.$table_name" + "?page=$pgnm&pagesize=1000"
        Write-Host "Executing API pull from Powerschool for $table_name."

        do
        {
            Write-Host "Grabbing 1000 records for $table_name, page $pgnm."
            $api_url_prefix = "https://setoncatholic.powerschool.com/ws/schema/query/org.setoncatholic.$table_name" + "?page=$pgnm&pagesize=1000"
        
            if($table_name -eq "assignments")
                {$results = (Invoke-WebRequest -Headers $headers -Method POST -Body $termbody -Uri $api_url_prefix)}
            elseif($table_name -eq "assignment_scores")
                {$results = (Invoke-WebRequest -Headers $headers -Method POST -Body $termbody -Uri $api_url_prefix)}
            elseif($table_name -eq "CC")
                {$results = (Invoke-WebRequest -Headers $headers -Method POST -Body $yearbody -Uri $api_url_prefix)}
            else
                {$results = (Invoke-WebRequest -Headers $headers -Method POST -Uri $api_url_prefix)}
            
            $results_obj = ConvertFrom-Json($results.Content)
            $lines_to_commit=$lines_to_commit+$results_obj.record.Count ### increment number of records for periodic commit of records

            $lines = foreach ($rec in $results_obj.record)
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
                $name_string = $sqlrow_names -join ","
                $value_string = $sqlrow_values -join "','"
                'INSERT INTO ps_mirror.'+ $table_name + ' (' + $name_string + ') VALUES (''' + $value_string + ''');'
            }

            ### Increment to bring next 1000/page if there are any records left
            ++$pgnm

            if(($lines_to_commit -ge 4000) -or ($results_obj.record.Count -lt 1000))
            {
                $query_string = $lines -join "`r`n" ### Concatenate all array elements together
                Write-Host "Executing SQL query to add rows to $table_name."

                $sql_commit = ExecuteNonQuery -ConnectionString $cnx_string -command_string $query_string
                $lines_to_commit=0
            }
        } while ($results_obj.record.Count -eq 1000)

        Write-Host "Table $table_name complete."

    }
    catch
    {
        Write-Host "Error while running query $table_name"
        $error
    }
}

#endregion

#region Logging section
######################################

$EndDate=(GET-DATE)
$timespan = NEW-TIMESPAN -Start $StartDate -End $EndDate
"Run time: " + $timespan.TotalMinutes + " minutes"

$errortostore = $Error -replace "'", "''"
$query = "insert into job_logs.job_results values ('"+$EndDate+"','PS Mirror',"+$timespan.TotalMinutes+","+$error.Count+",'"+$errortostore+"')"
$results = (ExecuteNonQuery -ConnectionString $Env:PSQL_CONNECTION_STRING -command_string $query)

#endregion