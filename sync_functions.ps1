
. .\local_env_variables.ps1

function CanvasRESTCall($url, $method, $body, $event, $comment, $jobname)
{
    ### local mirror DB information
    $content = $null
    $response = $null

    ### Canvas key, header and URL for CSV call
    $api_key = "3925~lTUMZwIlZTksziBGPfzcyWgFD107hHGqs6CInmD9HTqsoaPmEdCIHygBw13XeQ4j"
    $headers = @{"Authorization"="Bearer "+$api_key;"Content-Type"="application/json; charset=utf-8"}

    $jobsuccess='true'
    try
    {
        if($method -eq "GET")
        {
            $nexturl=$url
            do
            {
                $results = Invoke-WebRequest -Headers $headers -Method $method -Uri $nexturl
                $nexturl=""
                if($results.Headers.Link)
                {
                    $header_links = $results.Headers.Link.Split(",")
                    foreach ($l in $header_links)
                    {
                        $linksplit = $l -split ";"
                        if ($linksplit[1] -eq ' rel="next"')
                        {
                            $nexturl=$linksplit[0].Trim("<").Trim(">")
                            ###Write-Host $nexturl
                        }
                    }
                }
                $response = $results.RawContent
                $content += ConvertFrom-Json $results.Content
            } while($nexturl)
        }
        elseif($method -eq "DELETE")
        {
            $results = Invoke-WebRequest -Headers $headers -Method $method -Uri $url
            $response = $results.RawContent
            $content = ConvertFrom-Json $results.Content
        }
        else
        {
            $results = Invoke-WebRequest -Headers $headers -Body $body -Method $method -Uri $url
            $response = $results.RawContent
            $content = ConvertFrom-Json $results.Content
        }
    }
    catch
    {
        $jobsuccess='false'
        $response = $_.Exception.ToString()
    }
    ###$response = $response -replace "'","''"
    $body_json = $body_json -replace "'","''"
    $query_string = "insert into job_logs.rest_events 
                        (time, event, comment, successful, url, body, response, method, jobname) 
                        values (CURRENT_TIMESTAMP, '$event','$comment', $jobsuccess, '$api_url','$body_json', '','$method','$jobname');"
    $store_event = (ExecuteNonQuery -ConnectionString $Env:PSQL_CONNECTION_STRING -command_string $query_string)
    return $content
}

function ExecuteQuery
{
    [CmdletBinding()]
    param (
        [Parameter(Mandatory=$true,HelpMessage="DSN Connection String")]
        [string[]]$ConnectionString,
        [Parameter(Mandatory=$true,HelpMessage="command to be executed",ValueFromPipeline=$true)]
        [string[]]$command_string
    )
    begin
    {
        $conn = New-Object System.Data.Odbc.OdbcConnection;
        $conn.ConnectionString = $ConnectionString
        $conn.Open()
    }
    process
    {
        $cmd = New-object System.Data.Odbc.OdbcCommand($command_string,$conn)
        $ds = New-Object system.Data.DataSet
        (New-Object system.Data.odbc.odbcDataAdapter($cmd)).fill($ds) | out-null
    }
    end
    {
        $ds.Tables[0]
        $conn.Close()
    }
}

function ExecuteNonQuery
{
    [CmdletBinding()]
    param (
        [Parameter(Mandatory=$true,HelpMessage="DSN Connection String")]
        [string[]]$ConnectionString,
        [Parameter(Mandatory=$true,HelpMessage="command to be executed",ValueFromPipeline=$true)]
        [string[]]$command_string
    )
    begin
    {
        $conn = New-Object System.Data.Odbc.OdbcConnection;
        $conn.ConnectionString = $ConnectionString
        $conn.Open()
    }
    process
    {
        $cmd = New-object System.Data.Odbc.OdbcCommand($command_string,$conn)
        $cmd.ExecuteNonQuery()
    }
    end
    {
        $conn.Close()
    }
}
