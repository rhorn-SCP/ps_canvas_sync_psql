function CanvasRESTCall($url, $method, $body, $event, $comment, $jobname)
{
    ### local mirror DB information
    $server_name = "SETON-SQL3"
    $db_name = "job_logs"
    $Username = "rhorn"
    $Password = "onAT4evase"
    $content = $null
    $response = $null

    ### Canvas key, header and URL for CSV call
    $api_key = "3925~lTUMZwIlZTksziBGPfzcyWgFD107hHGqs6CInmD9HTqsoaPmEdCIHygBw13XeQ4j"
    $headers = @{"Authorization"="Bearer "+$api_key;"Content-Type"="application/json; charset=utf-8"}

    $jobsuccess=1
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
                            Write-Host "And another..."
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
            $results = Invoke-WebRequest -Headers $headers -Body ([System.Text.Encoding]::UTF8.GetBytes($body)) -Method $method -Uri $url -ContentType 'application/json; charset=utf8'
            $response = $results.RawContent
            $content = ConvertFrom-Json $results.Content
        }
    }
    catch
    {
        $jobsuccess=0
        $response = $_.Exception.ToString()
    }
    $response = $response -replace "'","''"
    $body_json = $body_json -replace "'","''"
    $query_string = "insert into REST_events 
                        ([time], [event], [comment], [successful], [url], [body], [response],[method],[jobname]) 
                        values (getdate(), '$event','$comment', $jobsuccess, '$api_url','$body_json', '$response','$method','$jobname');"
    $store_event = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query_string -Username $Username -Password $Password
    return $content
}

function MirrorStatusCheck($query_results, $table_name, $server_name, $db_name, $Username, $Password)
{
    #### Set mirror status for table in config to true if query returns rows
    if($query_results)
    {
        $query_string=
        "
            update canvas_currentyear.dbo.config
            set value_b=1
            where value_a in ('"+$table_name+"') and config_type='canvasmirror'
        "
        $result=Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query_string -Username $Username -Password $Password
    }
}