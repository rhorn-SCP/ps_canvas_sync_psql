. .\local_env_variables.ps1

$StartDate=(GET-DATE)

#region Setup code - db names, paths, headers, etc

<#

### This snippet of code digitally signs the code (at the bottom). This will allow it to run with the SQL2 Code Signing Certficate.
### Must be run every time the code changes.

$cert_obj = dir Cert:\CurrentUser\My -CodeSigningCert
Set-AuthenticodeSignature "C:\Users\ryanh\Dropbox\teaching\miscellaneous\canvas\canvas_mirror\canvas_mirror_current.ps1" -Certificate $cert_obj

#### This following must be run to allow signed scripts to be run
Set-ExecutionPolicy -ExecutionPolicy AllSigned

#>

##### NOTE! PS_mirror should be run FIRST. ######

$error.Clear()

### Set procedure variables
$server_name = $Env:sqlservername
$db_name = "canvas_currentyear"
$Username = $Env:sql3user
$Password = $Env:sql3password
$api_key = $Env:canvasAPIkey
$headers = @{"Authorization"="Bearer "+$api_key}

Import-Module SqlServer

#### Which tables to mirror
$query_string="select config.value_a table_name, config.value_b needs_mirror from config where config_type='canvasmirror'"
$mirror_status = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query_string -Username "rhorn" -Password "onAT4evase" -ErrorAction Stop -QueryTimeout 600
$table_statuses =@{}
foreach ($stat in $mirror_status) {$table_statuses += @{$stat.table_name=$stat.needs_mirror}}

#endregion

#region IMPORT ACCOUNTS
#########################################################################
#########################################################################

if($table_statuses.accounts -eq 1)
{
    $table_name = "accounts"

    <#
    account object:

    "id": 2,
    "name": "Manually-Created Courses",
    "workflow_state": "active",
    "parent_account_id": 1,
    "root_account_id": 1,
    "uuid": "rE8a5S4fQqMKDYv2YjFSBpY29brE2zjFgzNA7bCw",
    "default_storage_quota_mb": 1000,
    "default_user_storage_quota_mb": 50,
    "default_group_storage_quota_mb": 100,
    "default_time_zone": "America/Phoenix",
    "sis_account_id": null,
    "sis_import_id": null,
    "integration_id": null

    #>

    ### Initialize the SQL Text object array and then drop and create table
    $add = @()
    $add += "IF OBJECT_ID('dbo.$table_name', 'U') IS NOT NULL DROP TABLE " + $table_name + ";"
    $add += "CREATE TABLE " + $table_name + "(
                                id varchar(255),
                                name varchar(255),
                                workflow_state varchar(255),
                                parent_account_id varchar(255),
                                root_account_id varchar(255),
                                uuid varchar(255),
                                default_storage_quota_mb varchar(255),
                                default_user_storage_quota_mb varchar(255),
                                default_group_storage_quota_mb varchar(255),
                                default_time_zone varchar(255),
                                sis_account_id varchar(255),
                                sis_import_id varchar(255),
                                integration_id varchar(255))"


    $api_url_prefix = "https://setoncatholic.instructure.com/api/v1/accounts/1/sub_accounts?page=1&per_page=1000"
    $results = (Invoke-WebRequest -Headers $headers -Method GET -Uri $api_url_prefix)
        
    ### Convert from JSON
    $results = ConvertFrom-Json $results

    ### Create T-SQL statement to execute
    foreach($r in $results)
    {
            $values =   $r.id,
                        $r.name,
                        $r.workflow_state,
                        $r.parent_account_id,
                        $r.root_account_id,
                        $r.uuid,
                        $r.default_storage_quota_mb,
                        $r.default_user_storage_quota_mb,
                        $r.default_group_storage_quota_mb,
                        $r.default_time_zone,
                        $r.sis_account_id,
                        $r.sis_import_id,
                        $r.integration_id
            $values_string = $values -join "','"
            $rec_to_add = "INSERT INTO " + $table_name + " VALUES ('" + $values_string + "')"  
            $add += $rec_to_add
    }
    
    $query_string = $add -join "`r`n" ### Concatenate all array elements together
    Write-Host "Executing SQL query to create $table_name."
    try
    {
        $sql_commit = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query_string -Username $Username -Password $Password -ErrorAction Stop -QueryTimeout 600
    }
    catch
    {
        Write-Host $error
        #[Windows.Forms.Clipboard]::SetText($query_string) ### Stores SQL string to clipboard!
    }
    Write-Host "Accounts complete."
}

#endregion

#region IMPORT TERMS
#########################################################################
#########################################################################

if($table_statuses.terms -eq 1)
{
    $table_name = "terms"

    ### Initialize the SQL Text object array and then drop and create table
    $add = @()
    $add += "IF OBJECT_ID('dbo.$table_name', 'U') IS NOT NULL DROP TABLE " + $table_name + ";"
    $add += "CREATE TABLE " + $table_name + "(
                                id varchar(255),
                                name varchar(255),
                                end_at varchar(255),
                                created_at varchar(255),
                                workflow_state varchar(255),
                                sis_term_id varchar(255),
                                sis_import_id varchar(255))"


    Write-Host "Starting terms."
    $api_url_prefix = "https://setoncatholic.instructure.com/api/v1/accounts/1/terms?page=1&per_page=1000"
    $results = (Invoke-WebRequest -Headers $headers -Method GET -Uri $api_url_prefix)

    ### Convert from JSON
    $results = ConvertFrom-Json $results

    ### Create T-SQL statement to execute
    foreach($r in $results.enrollment_terms)
    {
            $values =   $r.id,
                        $r.name,
                        $r.end_at,
                        $r.created_at,
                        $r.workflow_state,
                        $r.sis_term_id,
                        $r.sis_import_id
            $values_string = $values -join "','"
            $rec_to_add = "INSERT INTO " + $table_name + " VALUES ('" + $values_string + "')"  
            $add += $rec_to_add
    }
    
    $query_string = $add -join "`r`n" ### Concatenate all array elements together
    Write-Host "Executing SQL query to create $table_name."
    try
    {
        $sql_commit = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query_string -Username $Username -Password $Password -ErrorAction Stop -QueryTimeout 600
    }
    catch
    {
        Write-Host $error
        ##[Windows.Forms.Clipboard]::SetText($query_string) ### Stores SQL string to clipboard!
    }
    Write-Host "Terms complete."

}
#endregion

#region IMPORT USERS
#########################################################################
#########################################################################

if($table_statuses.users -eq 1)
{
    $table_name = "users"
    $teachers = @()
    $students = @()
    $observers = @()

    ### Initialize the SQL Text object array and then drop and create table
    $add = @()
    $add += "IF OBJECT_ID('dbo.$table_name', 'U') IS NOT NULL DROP TABLE " + $table_name
    $add += "CREATE TABLE $table_name (
                            sis_user_id nvarchar(255),
                            login_id nvarchar(255),
                            full_name nvarchar(255),
                            sortable_name nvarchar(255),
                            id nvarchar(255))"
    Write-Host "Starting users"
    $pgnum = 1
    $fetched = 0

    DO
    {
        $api_url_prefix = "https://setoncatholic.instructure.com/api/v1/accounts/self/users?page=$pgnum&per_page=100"
        $results = (Invoke-WebRequest -Headers $headers -Method GET -Uri $api_url_prefix)
        $results = ConvertFrom-Json $results

        ### Create T-SQL statement to execute
        foreach($r in $results)
        {
            $scrubbed_name = $r.name -replace "'","''"
            $scrubbed_sortable_name = $r.sortable_name -replace "'","''"
            $values = $r.sis_user_id,$r.login_id,$scrubbed_name,$scrubbed_sortable_name,$r.id
            $values_string = $values -join "','"
            $rec_to_add = "INSERT INTO " + $table_name + " VALUES ('" + $values_string + "')"  
            ##if($r.sis_user_id -ne $null) {$add += $rec_to_add}
            $add += $rec_to_add

            if($r.sis_user_id) {$sisuserid = $r.sis_user_id.Substring(0,4)}
            else {$sisuserid = ""}
            switch ($sisuserid)
            {
                'udci' {$teachers += $r.id;$observers += $r.id} ### add staff id to array for use later
                'stud' {$students += $r.id} ### add student id to array for use later
                default {$observers += $r.id} ### is an observer account, ignore
            }
        }
        ++$pgnum
        $fetched = $fetched + $results.Count
        Write-Host "Fetching at $fetched users."
    } while($results.Count -eq 100)

    $query_string = $add -join "`r`n" ### Concatenate all array elements together
    Write-Host "Executing SQL query to create $table_name."
    try
    {
        $sql_commit = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query_string -Username $Username -Password $Password -ErrorAction Stop -QueryTimeout 600
    }
    catch
    {
        Write-Host $error
        #[Windows.Forms.Clipboard]::SetText($query_string) ### Stores SQL string to clipboard!
    }
    Write-Host "Users complete."
}

#endregion

#region IMPORT OBSERVEES
#########################################################################
#########################################################################

if($table_statuses.observees -eq 1)
{
    <#

    observee object:

    id                                : 1618
    name                              : Jacob Garcia
    created_at                        : 2016-05-19T16:16:01-07:00
    sortable_name                     : Garcia, Jacob
    short_name                        : Jacob Garcia
    sis_user_id                       : student_324
    integration_id                    : 
    sis_import_id                     : 458
    root_account                      : setoncatholic.instructure.com
    login_id                          : jgarcia20@setoncatholic.org
    observation_link_root_account_ids : {1}

    #>

    $table_name = "observees"
    $query_string =
    "
    select distinct *
    from users
    where left(login_id,8)<> 'DISABLED'
    "
    ### sis_user_id not in (select distinct sis_user_id from enrollments where role_id=3) and 
    
    $users = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query_string -Username $Username -Password $Password -QueryTimeout 600

    ### Initialize the SQL Text object array and then drop and create table
    $add = @()
    $add += "IF OBJECT_ID('dbo.$table_name', 'U') IS NOT NULL DROP TABLE " + $table_name
    $add += "CREATE TABLE $table_name (
                    observer_id                       varchar(255),
                    observer_sis_user_id              varchar(255),
                    id                                varchar(255),
                    name                              varchar(255),
                    created_at                        varchar(255),
                    sortable_name                     varchar(255),
                    short_name                        varchar(255),
                    sis_user_id                       varchar(255),
                    integration_id                    varchar(255), 
                    sis_import_id                     varchar(255),
                    root_account                      varchar(255),
                    login_id                          varchar(255),
                    observation_link_root_account_ids varchar(255))"
    $rec_count = 0
    $reccount_max = $users.Count
    Write-Host "Starting observees."

    foreach ($u in $users)
    { 
            $rec_count++
            if ($rec_count % 50 -eq 0)
            {
                Write-Host "Enumerating users for observees, at record $rec_count of $reccount_max."
            }

            $api_url_prefix = "https://setoncatholic.instructure.com/api/v1/users/"+$u.id+"/observees?page=1&per_page=500"
            $observees = (Invoke-WebRequest -Headers $headers -Method GET -Uri $api_url_prefix)
            $observees = ConvertFrom-Json $observees
        
            if ($observees.Count -gt 0)
            {
                foreach ($ob in $observees)
                {
                    $values_array = @($u.id -replace "'","''")
                    $values_array += @($u.sis_user_id -replace "'","''")
                    foreach($f in $ob.psobject.properties)
                    {
                        $values_array += $f.Value -replace "'","''"
                    }
                    $values_string = $values_array -join "','"
                    $rec_to_add = "INSERT INTO " + $table_name + " VALUES ('" + $values_string + "')"  
                    $add += $rec_to_add
                }
            }
    }

    $query_string = $add -join "`r`n" ### Concatenate all array elements together
    Write-Host "Executing SQL query to create $table_name."
    try
    {
        $sql_commit = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query_string -Username $Username -Password $Password -ErrorAction Stop -QueryTimeout 600
        Write-Host "Observees complete."
    }
    catch
    {
        Write-Host $error
        #[Windows.Forms.Clipboard]::SetText($query_string) ### Stores SQL string to clipboard!
        Write-Host "Observees failed."
    }
}
#endregion

#region IMPORT COURSES
#########################################################################
#########################################################################

###Read-Host -Prompt "Press Enter to continue"

if($table_statuses.courses -eq 1)
{
    ########## Now pull terms from config table to use to grab courses we are syncing #############
    $query="
            select
	            value_a ps_termid,
	            value_b canvas_termid
            from config
            where config_type='syncterms'
        "

    try
    {
        $terms = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query -Username $Username -Password $Password -QueryTimeout 600
    }
    catch
    {
        Write-Host $error
    }

    $table_name = "courses"

    ### Initialize the SQL Text object array and then drop and create table
    $add = @()
    $add += "IF OBJECT_ID('dbo.$table_name', 'U') IS NOT NULL DROP TABLE " + $table_name + ";"
    $add += "CREATE TABLE " + $table_name + "(
                                sis_course_id varchar(255),
                                short_name varchar(255),
                                long_name varchar(255),
                                account_id varchar(255),
                                enrollment_term_id varchar(255),
                                id varchar(255),
                                workflow_state varchar(255),
                                apply_assignment_group_weights varchar(255))"

    Write-Host "Starting courses."
    foreach ($t in $terms.canvas_termid)
    {
        Write-Host "Fetching courses for term $t."
        $pgnum=1
        DO
        {
            $api_url_prefix = "https://setoncatholic.instructure.com/api/v1/accounts/1/courses?page=$pgnum&per_page=100&enrollment_term_id=$t"
            $results = (Invoke-WebRequest -Headers $headers -Method GET -Uri $api_url_prefix)
            $results = ConvertFrom-Json $results ### Convert from JSON

            ### Create T-SQL statement to execute
            foreach($r in $results)
            {
                    $scrubbed_course_id = $r.sis_course_id -replace "'","''"
                    $scrubbed_name = $r.name -replace "'","''"
                    $scrubbed_course_code = $r.course_code -replace "'","''"
                    $values =   $scrubbed_course_id,
                                $scrubbed_course_code,
                                $scrubbed_name,
                                $r.account_id,
                                $t,
                                $r.id,
                                $r.workflow_state,
                                $r.apply_assignment_group_weights
                    $values_string = $values -join "','"
                    $rec_to_add = "INSERT INTO " + $table_name + " VALUES ('" + $values_string + "')"
                    $add += $rec_to_add
            }
            ++$pgnum
        } while($results.Count -eq 100)
    }

    $query_string = $add -join "`r`n" ### Concatenate all array elements together
    Write-Host "Executing SQL query to create $table_name."
    try
    {
        $sql_commit = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query_string -Username $Username -Password $Password -ErrorAction Stop -QueryTimeout 600
    }
    catch
    {
        Write-Host $error
    }
    Write-Host "Courses complete."
}

#endregion

#region IMPORT SECTIONS
#########################################################################
#########################################################################

if($table_statuses.sections -eq 1)
{
    $table_name = "sections"

    ### Grab all the courses from database. Put it into $courses
    $courses = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query "select distinct id from courses" -Username $Username -Password $Password -QueryTimeout 600

    ### Initialize the SQL Text object array and then drop and create table
    $sections = @()

    $sections += "IF OBJECT_ID('dbo.$table_name', 'U') IS NOT NULL DROP TABLE " + $table_name + ";"
    $sections += "CREATE TABLE $table_name (sis_section_id nvarchar(255),
                                            sis_course_id nvarchar(255),
                                            name nvarchar(255),
                                            course_id nvarchar(255),
                                            id nvarchar(255),
                                            nonxlist_course_id nvarchar(255))"
    $rec_count = 0
    $reccount_max = $courses.Count
    Write-Host "Starting sections."

    foreach($id in $courses.id)
    {
        $rec_count++
        if ($rec_count % 50 -eq 0)
        {
            Write-Host "Enumerating courses for sections, at record $rec_count of $reccount_max."
        }
        $api_url_prefix = "https://setoncatholic.instructure.com/api/v1/courses/$id/sections"
        $results = (Invoke-WebRequest -Headers $headers -Method GET -Uri $api_url_prefix)

        ### Convert from JSON
        $results = ConvertFrom-Json $results

        ### Create T-SQL statement to execute
        foreach($r in $results)
        {
            ### 
            if($r.sis_section_id -ne $null)
            {
                $scrubbed_sis_course_id = $r.sis_course_id -replace "'","''"
                $scrubbed_sis_section_id = $r.sis_section_id -replace "'","''"
                $scrubbed_name = $r.name -replace "'","''"
                $values =   $scrubbed_sis_section_id,
                            $scrubbed_sis_course_id,
                            $scrubbed_name,
                            $r.course_id,
                            $r.id,
                            $r.nonxlist_course_id
                $values_string = $values -join "','"
                $rec_to_add = "INSERT INTO " + $table_name + " VALUES ('" + $values_string + "')"  
                $sections += $rec_to_add
            }
        }
    }

    $query_string = $sections -join "`r`n" ### Concatenate all array elements together
    Write-Host "Executing SQL query to create $table_name."
    try
    {
        $sql_commit = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query_string -Username $Username -Password $Password -ErrorAction Stop -QueryTimeout 600
    }
    catch
    {
        Write-Host $error
    }
    Write-Host "Sections complete."
}

#endregion

#region MIRROR ENROLLMENTS
#########################################################################
#########################################################################

if($table_statuses.enrollments -eq 1)
{
    $table_name = "enrollments"

    ### Grab all the sections from database. Put it into $sections
    $sections = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query "select distinct id from sections" -Username $Username -Password $Password -QueryTimeout 600

    ### Initialize the SQL Text object array and then drop and create table
    $enrollments = @()

    $enrollments += "IF OBJECT_ID('dbo.$table_name', 'U') IS NOT NULL DROP TABLE " + $table_name + ";"
    $enrollments += "CREATE TABLE $table_name (
                        sis_course_id nvarchar(255),
                        sis_section_id nvarchar(255),
                        user_id nvarchar(255),
                        associated_user_id nvarchar(255),
                        sis_user_id nvarchar(255),
                        sortable_name nvarchar(255),
                        role_id nvarchar(255),
                        role_type nvarchar(255),
                        section_id nvarchar(255),
                        course_id nvarchar(255),
                        user_url nvarchar(255),
                        id nvarchar(255));"

    $rec_count = 0
    $reccount_max = $sections.Count
    Write-Host "Starting enrollments."

    ### Grab all enrollments for each section_id.
    foreach($id in $sections.id)
    {
        $rec_count++
        if ($rec_count % 50 -eq 0)
        {
            Write-Host "Enumerating sections for enrollments, at record $rec_count of $reccount_max."
        }


        $nexturl = "https://setoncatholic.instructure.com/api/v1/sections/$id/enrollments?page=1&per_page=1000&type[]=StudentEnrollment&type[]=TeacherEnrollment&state[]=active"

        DO
        {
            $results = (Invoke-WebRequest -Headers $headers -Method GET -Uri $nexturl)
            $header_links = $results.Headers.Link.Split(",")
            $nexturl=""
            foreach ($l in $header_links)
            {
                $linksplit = $l -split ";"
                if ($linksplit[1] -eq ' rel="next"')
                {
                    $nexturl=$linksplit[0].Trim("<").Trim(">")
                }
            }
                
            if($results.Content.Length -gt 2)
            {
                $rows = ConvertFrom-Json $results.Content ### Convert from JSON

                ### Create T-SQL statement to execute
                foreach($r in $rows)
                {
                    if($r.sis_section_id -ne $null)
                    {
                        $scrubbed_sis_course_id = $r.sis_course_id -replace "'","''"
                        $scrubbed_sis_section_id = $r.sis_section_id -replace "'","''"
                        $scrubbed_sis_user_id = $r.user.sis_user_id -replace "'","''"
                        $scrubbed_sortable_name = $r.user.sortable_name -replace "'","''"

                        $values =
                            $scrubbed_sis_course_id,
                            $scrubbed_sis_section_id,
                            $r.user_id,
                            $r.associated_user_id,
                            $scrubbed_sis_user_id,
                            $scrubbed_sortable_name,
                            $r.role_id,
                            $r.type,
                            $r.course_section_id,
                            $r.course_id,
                            $r.html_url,
                            $r.id
                        $values_string = $values -join "','"
                        $rec_to_add = "INSERT INTO " + $table_name + " VALUES ('" + $values_string + "')"
                        $enrollments += $rec_to_add
                    }
                }
            }
        } while($nexturl)

    }

    $query_string = $enrollments -join "`r`n" ### Concatenate all array elements together

    try
    {
        $sql_commit = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query_string -Username $Username -Password $Password -ErrorAction Stop -QueryTimeout 600
    }
    catch
    {
        Write-Host $error
    }

    Write-Host "Enrollments complete."
}

#endregion

#region Canvas grade mirror
if($table_statuses.grades -eq 1)
{

    #########################################################################
    # IMPORT GRADES
    #########################################################################

    $table_name = "grades_active"
    $query_string =
    "
        select distinct s.id
        from sections s
        inner join courses c
	        on c.id=s.course_id
        where c.enrollment_term_id in (SELECT value_b FROM dbo.config WHERE config_type = 'syncterms') and c.workflow_state='available'
    "
    try
    {
        $sections = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query_string -Username $Username -Password $Password -QueryTimeout 600
    }
    catch
    {
        Write-Host $error
    }

    ### Initialize the SQL Text object array and then drop and create table
    $grades = @()
    $grades += "IF OBJECT_ID('dbo.$table_name', 'U') IS NOT NULL DROP TABLE " + $table_name + ";"
    $grades += "CREATE TABLE $table_name (
                        sis_course_id nvarchar(255),
                        sis_section_id nvarchar(255),
                        user_id nvarchar(255),
                        sis_user_id nvarchar(255),
                        sortable_name nvarchar(255),
                        role_id nvarchar(255),
                        role_type nvarchar(255),
                        section_id nvarchar(255),
                        course_id nvarchar(255),
                        user_url nvarchar(255),
                        grade nvarchar(255),
                        id nvarchar(255));"

    $recordcount = 0
    $totalsections = $sections.Count
    foreach($id in $sections.id)
    {
        $recordcount = $recordcount + 1
        Write-Host "Grabbing grades for section_id: $id. ($recordcount/$totalsections)"
        
        $nexturl = "https://setoncatholic.instructure.com/api/v1/sections/$id/enrollments?state[]=active&type=StudentEnrollment&page=1&per_page=1000"
        $content = $null
        do {
            $results = Invoke-WebRequest -Headers $headers -Method "GET" -Uri $nexturl
            $header_links = $results.Headers.Link.Split(",")
            $nexturl=""
            foreach ($l in $header_links)
            {
                $linksplit = $l -split ";"
                if ($linksplit[1] -eq ' rel="next"')
                    {$nexturl=$linksplit[0].Trim("<").Trim(">")}
            }
            $content += ConvertFrom-Json $results.Content
        } while($nexturl)

        ### Create T-SQL statement to execute
        foreach($r in $content)
        {
            $grade=$r.grades.current_score -replace "'","''"
            $scrubbed_sis_course_id = $r.sis_course_id -replace "'","''"
            $scrubbed_sis_section_id = $r.sis_section_id -replace "'","''"
            $scrubbed_sis_user_id = $r.user.sis_user_id -replace "'","''"
            $scrubbed_sortable_name = $r.user.sortable_name -replace "'","''"

            $values =
                $scrubbed_sis_course_id,
                $scrubbed_sis_section_id,
                $r.user_id,
                $scrubbed_sis_user_id,
                $scrubbed_sortable_name,
                $r.role_id,
                $r.type,
                $r.course_section_id,
                $r.course_id,
                $r.html_url,
                [math]::Round($grade,2, [system.midpointrounding]::AwayFromZero),
                $r.id                        
            $values_string = $values -join "','"
            $rec_to_add = "INSERT INTO " + $table_name + " VALUES ('" + $values_string + "')"
            $grades += $rec_to_add
        }
    }

    $query_string = $grades -join "`r`n" ### Concatenate all array elements together
    #[Windows.Forms.Clipboard]::SetText($query_string)
    try
    {
        $sql_commit = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query_string -Username $Username -Password $Password -ErrorAction Stop -QueryTimeout 600
    }
    catch
    {
        Write-Host $error
        #[Windows.Forms.Clipboard]::SetText($query_string) ### Stores SQL string to clipboard!
    }
    Write-Host "Grades complete."
}
#endregion

#region MIRROR ALL ASSIGNMENTS/ASSIGNMENT SUBMISSIONS/SCORES
#########################################################################
#########################################################################

if($table_statuses.assignments -eq 1)
{
    $query_string = "select * from courses where enrollment_term_id in (SELECT value_b FROM dbo.config WHERE config_type = 'syncterms')"
    $courses = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query_string -Username $Username -Password $Password -ErrorAction Stop
    $courses.Count
    $course_count=0

    <#
    assignment object:
    {
        "id": 91710,
        "due_at": "2019-08-12T06:59:59Z",
        "points_possible": 11.0,
        "grading_type": "points",
        "assignment_group_id": 6359,
        "created_at": "2019-08-07T16:31:18Z",
        "updated_at": "2019-08-12T18:04:41Z",
        "omit_from_final_grade": false,
        "course_id": 2224,
        "name": "IG.1 - Project Ideas",
        "workflow_state": "published",
        "muted": false,
        "html_url": "https://setoncatholic.instructure.com/courses/2224/assignments/91710",
        "sis_assignment_id": null,
        "published": true
    }

    assignment_submission
    {
        "id": 3144145,
        "body": null,
        "url": null,
        "grade": "5",
        "score": 5.0,
        "submitted_at": null,
        "assignment_id": 91711,
        "user_id": 1599,
        "submission_type": null,
        "workflow_state": "graded",
        "grade_matches_current_submission": true,
        "graded_at": "2019-08-15T14:25:29Z",
        "grader_id": 2,
        "attempt": null,
        "cached_due_date": "2019-08-13T06:59:59Z",
        "excused": false,
        "late_policy_status": null,
        "points_deducted": null,
        "grading_period_id": null,
        "extra_attempts": null,
        "late": false,
        "missing": false,
        "seconds_late": 2543028,
        "entered_grade": "5",
        "entered_score": 5.0,
        "preview_url": "https://setoncatholic.instructure.com/courses/2224/assignments/91711/submissions/1599?preview=1&version=1",
        "anonymous_id": "8QK9T"
    }

        #>

        ### drop existing table and recreate empty one ###
        $query_string = 
        "
        IF OBJECT_ID('dbo.assignments', 'U') IS NOT NULL DROP TABLE assignments;
        CREATE TABLE assignments(
                id nvarchar(255),
                description nvarchar(max),
                due_at datetime,
                points_possible nvarchar(255),
                grading_type nvarchar(255),
                assignment_group_id nvarchar(255),
                created_at datetime,
                updated_at datetime,
                omit_from_final_grade nvarchar(255),
                course_id nvarchar(255),
                name nvarchar(255),
                workflow_state nvarchar(255),
                muted nvarchar(255),
                html_url nvarchar(255),
                sis_assignment_id nvarchar(255),
                published nvarchar(255));

        IF OBJECT_ID('dbo.assignment_submissions', 'U') IS NOT NULL DROP TABLE assignment_submissions;
        CREATE TABLE assignment_submissions(
                id nvarchar(255),
                grade nvarchar(255),
                score nvarchar(255),
                submitted_at datetime,
                assignment_id nvarchar(255),
                user_id nvarchar(255),
                workflow_state nvarchar(255),
                grade_matches_current_submission nvarchar(255),
                graded_at datetime,
                grader_id nvarchar(255),
                excused nvarchar(255),
                late nvarchar(255),
                missing nvarchar(255),
                entered_grade nvarchar(255),
                entered_score nvarchar(255),
                preview_url nvarchar(255));
        "
        $result = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query_string -Username $Username -Password $Password -ErrorAction Stop

        $rows = @()

        foreach ($course in $courses)
        {
            $pgnum=1
            $course_count++
            $nexturl="https://setoncatholic.instructure.com/api/v1/courses/"+$course.id+"/assignments?per_page=1000"

            do
            {
                $result = (Invoke-WebRequest -Headers $headers -Method GET -Uri $nexturl -ErrorAction Stop)

                $header_links = $result.Headers.Link.Split(",")
                $nexturl=""
                foreach ($l in $header_links)
                {
                    $linksplit = $l -split ";"
                    if ($linksplit[1] -eq ' rel="next"') {$nexturl=$linksplit[0].Trim("<").Trim(">")}
                }
                
                if($result.Content.Length -gt 2)
                {
                    $assignments= ConvertFrom-Json $result.Content

                    foreach($assignment in $assignments)
                    {
                        if($assignment.published -eq "true")
                        {
                            $assignment_name = $assignment.name -replace "'","''"
                            $assignment_description = $assignment.description -replace "'","''"
                            $assignment_name = $assignment_name.Trim()
                            $values =   $assignment.id,
                                        $assignment_description,
                                        $assignment.due_at,
                                        $assignment.points_possible,
                                        $assignment.grading_type,
                                        $assignment.assignment_group_id,
                                        $assignment.created_at,
                                        $assignment.updated_at,
                                        $assignment.omit_from_final_grade,
                                        $assignment.course_id,
                                        $assignment_name,
                                        $assignment.workflow_state,
                                        $assignment.muted,
                                        $assignment.html_url,
                                        $assignment.sis_assignment_id,
                                        $assignment.published
                            $values_string = $values -join "','"
                            $rec_to_add = "INSERT INTO assignments VALUES ('" + $values_string + "')"
                            $rec_to_add = $rec_to_add -replace ",'',", ",null,"
                            $rows += $rec_to_add
                        }
                    }
                }
            }
            while ($nexturl)

            if($rows.Count -ne 0)
            {
                $query_string = $rows -join "`r`n" ### Concatenate all array elements together
                
                try
                {
                    $sql_commit = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query_string -Username $Username -Password $Password -ErrorAction SilentlyContinue -QueryTimeout 600
                }
                catch
                {
                    Write-Host $error
                }

                $query_string = ""
                $rows = @()
            }

            $course_count.ToString() + " of " + $courses.Count.ToString() + ": ASSIGNMENTS completed for course "+$course.id+": "+$course.long_name

            ### Pull all submissions for each course
            $lines_to_commit=0
            $nexturl = "https://setoncatholic.instructure.com/api/v1/courses/"+$course.id+"/students/submissions?per_page=1000&student_ids[]=all&workflow_state=graded"

            do
            {
                $result = (Invoke-WebRequest -Headers $headers -Method GET -Uri $nexturl -ErrorAction Stop)

                $header_links = $result.Headers.Link.Split(",")
                $nexturl=""
                foreach ($l in $header_links)
                {
                    $linksplit = $l -split ";"
                    if ($linksplit[1] -eq ' rel="next"') {$nexturl=$linksplit[0].Trim("<").Trim(">")}
                }

                if($result.Content.Length -gt 2)
                {
                    $submissions = ConvertFrom-Json $result.Content
                    $lines_to_commit = $lines_to_commit + $submissions.Count

                    foreach($sub in $submissions)
                    {
                        if($sub.workflow_state -eq "graded")
                        {
                            $scrubbed_score = switch ($sub.entered_score)
                                {
                                    $null {"--"; break}
                                    "" {"--"; break}
                                    default {[math]::Round($sub.entered_score,2); break}
                                }
                            $values = 
                                $sub.id,
                                $sub.grade,
                                $sub.score,
                                $sub.submitted_at,
                                $sub.assignment_id,
                                $sub.user_id,
                                $sub.workflow_state,
                                $sub.grade_matches_current_submission,
                                $sub.graded_at,
                                $sub.grader_id,
                                $sub.excused,
                                $sub.late,
                                $sub.missing,
                                $sub.entered_grade,
                                $scrubbed_score,
                                $sub.preview_url
                            $values_string = $values -join "','"
                            $rec_to_add = "INSERT INTO assignment_submissions VALUES ('" + $values_string + "')"  
                            $rec_to_add = $rec_to_add -replace ",'',", ",null,"
                            $rows += $rec_to_add
                        }
                    }

                }

                if($rows.Count -ne 0)
                {
                    #### Need to commit periodically to ensure query string doesn't get too big
                    if(($lines_to_commit -ge 5000) -or ($nexturl -eq ""))
                    {
                        $query_string = $rows -join "`r`n" ### Concatenate all array elements together

                        try
                        {
                            $sql_commit = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query_string -Username $Username -Password $Password -ErrorAction SilentlyContinue -Querytimeout 600
                            $lines_to_commit=0
                            $rows=@()
                        }
                        catch
                        {
                            Write-Host $error
                        }
                    }
                }

            } while ($nexturl)

            $course_count.ToString() + " of " + $courses.Count.ToString() + ": SUBMISSIONS completed for course "+$course.id+": "+$course.long_name
        }

        Write-Host "Assignments and submissions complete."
}

#endregion

### Reset mirror status so that we can save on mirror run times
$query_string=
"
    update canvas_currentyear.dbo.config
    set value_b=0
    where value_a in ('users','observees','courses','sections','enrollments') and config_type='canvasmirror'
"
$result = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query_string -Username $Username -Password $Password

$EndDate=(GET-DATE)
$timespan = NEW-TIMESPAN –Start $StartDate –End $EndDate
"Run time: " + $timespan.TotalMinutes + " minutes"

$errortostore = $Error -replace "'", "''"
$query = "insert into job_results values ('"+$EndDate+"','Canvas Mirror',"+$timespan.TotalMinutes+","+$error.Count+",'"+$errortostore+"')"
$results = (Invoke-Sqlcmd -ServerInstance $server_name -database "job_logs" -query $query -Username $Username -Password $Password -QueryTimeout 600)


