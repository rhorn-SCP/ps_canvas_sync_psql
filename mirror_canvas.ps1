function CommitTable()
{
    [CmdletBinding()]
    param (
        [Parameter(Mandatory=$true,HelpMessage="table name")]
        [string[]]$table_name,
        [Parameter(Mandatory=$true,HelpMessage="DSN Connection String")]
        [string[]]$cnx_string,
        [Parameter(Mandatory=$true,HelpMessage="row value array",ValueFromPipeline=$true)]
        [string[]]$rowval_array,
        [Parameter(Mandatory=$true,HelpMessage="ordered column names",ValueFromPipeline=$true)]
        [string[]]$column_names
    )
    process
    {
        $records_string = $rowval_array -join ","
        $query_string = "TRUNCATE TABLE canvas_currentyear." + $table_name + ";`r`n"
        $query_string += "INSERT INTO canvas_currentyear." + $table_name + " " + $column_names + "`r`nVALUES`r`n" + $records_string + ";"
        $query_string = $query_string -replace ",''", ",null" ### replace '' with null
        
        Write-Host "Executing SQL query to create $table_name."
        try
        {
            $sql_commit = (ExecuteNonQuery -ConnectionString $cnx_string -command_string $query_string)
        }
        catch
        {
            Write-Host $error
        }
        Write-Host "$table_name complete."
    }
    end{}
}

$StartDate=(GET-DATE)

$error.Clear()
. .\local_env_variables.ps1
. .\sync_functions.ps1

#region Setup code - db names, paths, headers, etc
$termids = $Env:canvas_syncterms -split " "
$termidstring = "'" + ($termids -join "','") + "'"

#### Which tables to mirror
$query_string="select * from canvas_currentyear.config;"
$mirror_status = ExecuteQuery -command_string $query_string -ConnectionString $Env:PSQL_CONNECTION_STRING
#$mirror_status = $mirror_status[1..($mirror_status.Length-1)]
$table_statuses = foreach ($stat in $mirror_status) {@{$stat.table_name=$stat.needs_mirror}}
#endregion

#region IMPORT ACCOUNTS
#########################################################################
#########################################################################

if($table_statuses.accounts -eq 1)
{
    $table_name = "accounts"
    Write-Host "Starting $table_name."
    $api_url_prefix = "https://setoncatholic.instructure.com/api/v1/accounts/1/sub_accounts?per_page=1000"
    $results = (CanvasRESTCall -url $api_url_prefix -body "" -method "GET" -event "mirror.accounts" -comment "" -jobname "Canvas Mirror")

    $records_string = @()
    $records_string = foreach($r in $results)
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
            "('" + $values_string + "')`r`n"
    }
    
    $column_names = " (id,name,workflow_state,parent_account_id,root_account_id,uuid,default_storage_quota_mb,default_user_storage_quota_mb,default_group_storage_quota_mb,default_time_zone,sis_account_id,sis_import_id,integration_id)"
    $commit = CommitTable -table_name $table_name -cnx_string $Env:PSQL_CONNECTION_STRING -column_names $column_names -rowval_array $records_string
}

#endregion

#region IMPORT TERMS
#########################################################################
#########################################################################

if($table_statuses.terms -eq 1)
{
    $table_name = "terms"
    Write-Host "Starting $table_name."
    $api_url_prefix = "https://setoncatholic.instructure.com/api/v1/accounts/1/terms?per_page=1000"
    $results = (CanvasRESTCall -url $api_url_prefix -body "" -method "GET" -event "mirror.terms" -comment "" -jobname "Canvas Full Mirror")
    $records_string = @()

    ### Create T-SQL statement to execute
    $records_string = foreach($r in $results.enrollment_terms)
    {
            $values =   $r.id,
                        $r.name,
                        $r.start_at,
                        $r.end_at,
                        $r.created_at,
                        $r.workflow_state,
                        $r.grading_period_group_id,
                        $r.sis_term_id,
                        $r.sis_import_id
            $values_string = $values -join "','"
            "('" + $values_string + "')`r`n"
    }

    $column_names = " (id,name,start_at,end_at,created_at,workflow_state,grading_period_group_id,sis_term_id,sis_import_id)"
    $commit = CommitTable -table_name $table_name -cnx_string $Env:PSQL_CONNECTION_STRING -column_names $column_names -rowval_array $records_string
}
#endregion

#region IMPORT USERS
#########################################################################
#########################################################################

if($table_statuses.users -eq 1)
{
    $table_name = "users"
    Write-Host "Starting $table_name."
    $api_url_prefix = "https://setoncatholic.instructure.com/api/v1/accounts/self/users?per_page=1000"
    $results = (CanvasRESTCall -url $api_url_prefix -body "" -method "GET" -event "mirror.users" -comment "" -jobname "Canvas Full Mirror")
    $records_string = @()
    $rec_count = 0

    ### Create T-SQL statement to execute
    $records_string = foreach($r in $results)
    {
        $scrubbed_name = $r.name -replace "'","''"
        $scrubbed_sortable_name = $r.sortable_name -replace "'","''"
        $scrubbed_short_name = $r.short_name -replace "'","''"
        $values =   $r.id,
                    $scrubbed_name,
                    $r.created_at,
                    $scrubbed_sortable_name,
                    $scrubbed_short_name,
                    $r.sis_user_id,
                    $r.integration_id,
                    $r.sis_import_id,
                    $r.root_account,
                    $r.login_id                    
        $values_string = $values -join "','"
        "('" + $values_string + "')`r`n"

        $rec_count++
        if ($rec_count % 100 -eq 0)
        {
            Write-Host "At user record $rec_count."
        }
    }
    
    $column_names = " (id,name,created_at,sortable_name,short_name,sis_user_id,integration_id,sis_import_id,root_account,login_id)"
    $commit = CommitTable -table_name $table_name -cnx_string $Env:PSQL_CONNECTION_STRING -column_names $column_names -rowval_array $records_string
}

#endregion

#region IMPORT OBSERVEES
#########################################################################
#########################################################################

if($table_statuses.observees -eq 1)
{
    $table_name = "observees"
    Write-Host "Starting $table_name."
    $query_string = "select id, sis_user_id from canvas_currentyear.users where left(login_id,8)<> 'DISABLED' and (left(sis_user_id,7)<>'student' or sis_user_id is null);"
    $users = ExecuteQuery -command_string $query_string -ConnectionString $Env:PSQL_CONNECTION_STRING

    $rec_count = 0
    $reccount_max = $users.Count
    $records_string=@()

    $records_string = foreach ($u in $users)
    { 
            $rec_count++
            $api_url_prefix = "https://setoncatholic.instructure.com/api/v1/users/"+$u.id+"/observees?per_page=1000"
            $observees = @(CanvasRESTCall -url $api_url_prefix -body "" -method "GET" -event "mirror.observees" -comment "" -jobname "Canvas Full Mirror")

            if (($observees.Count -gt 0) -AND $observees )
            {
                foreach ($ob in $observees)
                {
                    $values_array = @($u.id -replace "'","''")
                    $values_array += $u.sis_user_id -replace "'","''"
                    $values_array += foreach($f in $ob.psobject.properties) {$f.Value -replace "'","''"}
                    $values_string = $values_array -join "','"
                    "('" + $values_string + "')`r`n"
                }
            }

            if ($rec_count % 100 -eq 0)
            {
                Write-Host "Enumerating users for observees, at user record $rec_count of $reccount_max."
            }
    }
    
    $column_names = " (observer_id,observer_sis_user_id,id,name,created_at,sortable_name,short_name,sis_user_id,integration_id,sis_import_id,root_account,login_id,observation_link_root_account_ids)"
    $commit = CommitTable -table_name $table_name -cnx_string $Env:PSQL_CONNECTION_STRING -column_names $column_names -rowval_array $records_string
}
#endregion

#region IMPORT COURSES
#########################################################################
#########################################################################

if($table_statuses.courses -eq 1)
{
    $table_name = "courses"
    Write-Host "Starting $table_name."
    $records_string = @()

    $records_string = foreach ($t in $termids)
    {
        Write-Host "Fetching courses for term $t."
        $api_url_prefix = "https://setoncatholic.instructure.com/api/v1/accounts/1/courses?per_page=100&enrollment_term_id=$t"
        $courses = @(CanvasRESTCall -url $api_url_prefix -body "" -method "GET" -event "mirror.courses" -comment "" -jobname "Canvas Full Mirror")

        ### Create T-SQL statement to execute
        foreach($r in $courses)
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
                "('" + $values_string + "')`r`n"
        }
    }
    
    $column_names = " (sis_course_id,short_name,long_name,account_id,enrollment_term_id,id,workflow_state,apply_assignment_group_weights)"
    $commit = CommitTable -table_name $table_name -cnx_string $Env:PSQL_CONNECTION_STRING -column_names $column_names -rowval_array $records_string
}

#endregion

#region IMPORT SECTIONS
#########################################################################
#########################################################################

if($table_statuses.sections -eq 1)
{
    ### Grab all the courses from database. Put it into $courses
    $query_string="select distinct id from canvas_currentyear.courses;"
    $courses = ExecuteQuery -command_string $query_string -ConnectionString $Env:PSQL_CONNECTION_STRING
    $table_name = "sections"
    Write-Host "Starting $table_name."
    $records_string = @()

    $rec_count = 0
    $reccount_max = $courses.Count

    $records_string = foreach($id in $courses.id)
    {
        $rec_count++
        if ($rec_count % 50 -eq 0)
        {
            Write-Host "Enumerating courses for sections, at record $rec_count of $reccount_max."
        }
        $api_url_prefix = "https://setoncatholic.instructure.com/api/v1/courses/$id/sections"
        $results = @(CanvasRESTCall -url $api_url_prefix -body "" -method "GET" -event "mirror.sections" -comment "" -jobname "Canvas Full Mirror")

        ### Create T-SQL statement to execute
        foreach($r in $results)
        {
            ### 
            if($null -ne $r.sis_section_id)
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
                "('" + $values_string + "')`r`n"
            }
        }
    }

    $column_names = " (sis_section_id,sis_course_id,name,course_id,id,nonxlist_course_id)"
    $commit = CommitTable -table_name $table_name -cnx_string $Env:PSQL_CONNECTION_STRING -column_names $column_names -rowval_array $records_string
}

#endregion

#region MIRROR ENROLLMENTS
#########################################################################
#########################################################################

if($table_statuses.enrollments -eq 1)
{
    $table_name = "enrollments"
    Write-Host "Starting $table_name."
    ### Grab all the sections from database. Put it into $courses
    $query_string="select distinct id from canvas_currentyear.sections;"
    $sections = ExecuteQuery -command_string $query_string -ConnectionString $Env:PSQL_CONNECTION_STRING
    $records_string = @()

    $rec_count = 0
    $reccount_max = $sections.Count

    ### Grab all enrollments for each section_id.
    $records_string += foreach($id in $sections.id)
    {
        $rec_count++
        if ($rec_count % 50 -eq 0) {Write-Host "Enumerating sections for enrollments, at record $rec_count of $reccount_max."}

        $api_url_prefix = "https://setoncatholic.instructure.com/api/v1/sections/$id/enrollments?per_page=1000&type[]=StudentEnrollment&type[]=TeacherEnrollment&state[]=active"
        $results = @(CanvasRESTCall -url $api_url_prefix -body "" -method "GET" -event "mirror.enrollments" -comment "" -jobname "Canvas Full Mirror")

        foreach($r in $results)
        {
            if($null -ne $r.sis_section_id)
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
                "('" + $values_string + "')`r`n"
            }
        }
    }

    $column_names = "
                    (
                        sis_course_id,
                        sis_section_id,
                        user_id,
                        associated_user_id,
                        sis_user_id,
                        sortable_name,
                        role_id,
                        role_type,
                        section_id,
                        course_id,
                        user_url,
                        id
                    )"
    $commit = CommitTable -table_name $table_name -cnx_string $Env:PSQL_CONNECTION_STRING -column_names $column_names -rowval_array $records_string
}

#endregion


#region Canvas grade mirror

if($table_statuses.grades -eq 1)
{

    #########################################################################
    # IMPORT GRADES
    #########################################################################

    $table_name = "grades"
    Write-Host "Starting $table_name."

    ### Grab all the courses from database. Put it into $courses
    $query_string=
    "
        select distinct s.id
        from canvas_currentyear.sections s
        inner join canvas_currentyear.courses c
            on c.id=s.course_id
        where c.enrollment_term_id in ($termidstring) and c.workflow_state='available';
    "
    $sections = ExecuteQuery -command_string $query_string -ConnectionString $Env:PSQL_CONNECTION_STRING
    
    $rec_count = 0
    $reccount_max = $sections.Count

    $records_string = @()
    $records_string = foreach($id in $sections.id)
    {
        $rec_count++
        Write-Host "Grabbing grades for section_id: $id. ($rec_count/$reccount_max)"
        $api_url_prefix = "https://setoncatholic.instructure.com/api/v1/sections/$id/enrollments?state=active&type=StudentEnrollment&per_page=1000"
        $results = @(CanvasRESTCall -url $api_url_prefix -body "" -method "GET" -event "mirror.sections" -comment "" -jobname "Canvas Full Mirror")

        foreach($r in $results)
        {
            if($null -ne $r.sis_section_id)
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
                "('" + $values_string + "')`r`n"
            }
        }

    }

    $column_names = " (sis_course_id,sis_section_id,user_id,sis_user_id,sortable_name,role_id,role_type,section_id,course_id,user_url,grade,id)"
    $commit = CommitTable -table_name $table_name -cnx_string $Env:PSQL_CONNECTION_STRING -column_names $column_names -rowval_array $records_string
}
#endregion

#region MIRROR ALL ASSIGNMENTS/ASSIGNMENT SUBMISSIONS/SCORES

if($table_statuses.assignments -eq 1)
{

    ##############################
    ### assignments  #############
    ##############################

    Write-Host "Starting $table_name."
    $table_name = "assignments"

    ### Grab all the courses from database. Put it into $courses
    $query_string=
    "select * from canvas_currentyear.courses where enrollment_term_id in ($termidstring)"
    $courses = ExecuteQuery -command_string $query_string -ConnectionString $Env:PSQL_CONNECTION_STRING
    $records_string = @()
    $rec_count = 0
    $reccount_max = $courses.Count

    foreach($id in $courses.id)
    {
        $rec_count++
        Write-Host "Grabbing assignments for course_id: $id. ($rec_count/$reccount_max)"
        $api_url_prefix = "https://setoncatholic.instructure.com/api/v1/courses/"+$id+"/assignments?per_page=1000"
        $results = @(CanvasRESTCall -url $api_url_prefix -body "" -method "GET" -event "mirror.assignments" -comment "" -jobname "Canvas Full Mirror")

        foreach($r in $results)
        {
            $assignment_name = $r.name -replace "'","''"
            $assignment_name = $assignment_name.Trim()
            $values =   $r.id,
                        $r.due_at,
                        $r.points_possible,
                        $r.grading_type,
                        $r.assignment_group_id,
                        $r.created_at,
                        $r.updated_at,
                        $r.omit_from_final_grade,
                        $r.course_id,
                        $assignment_name,
                        $r.workflow_state,
                        $r.muted,
                        $r.html_url,
                        $r.sis_assignment_id,
                        $r.published
            $values_string = $values -join "','"
            $records_string += "('" + $values_string + "')`r`n"
        }
    }

    $column_names = " (id,due_at,points_possible,grading_type,assignment_group_id,created_at,updated_at,omit_from_final_grade,course_id,name,workflow_state,muted,html_url,sis_assignment_id,published)"
    $commit = CommitTable -table_name $table_name -cnx_string $Env:PSQL_CONNECTION_STRING -column_names $column_names -rowval_array $records_string
    
    ##############################
    ### assignment submissions ###
    ##############################

    $table_name = "assignment_submissions"
    Write-Host "Starting $table_name."
    $records_string = @()
    $rec_count = 0
    $reccount_max = $courses.Count

    foreach($id in $courses.id)
    {
        $rec_count++
        Write-Host "Grabbing assignment submissions for course_id: $id. ($rec_count/$reccount_max)"
        $api_url_prefix = "https://setoncatholic.instructure.com/api/v1/courses/"+$id+"/students/submissions?per_page=1000&student_ids[]=all&workflow_state=graded"
        $results = @(CanvasRESTCall -url $api_url_prefix -body "" -method "GET" -event "mirror.assignment_submissions" -comment "" -jobname "Canvas Full Mirror")

        foreach($r in $results)
        {
            $scrubbed_score = switch ($r.entered_score)
            {
                $null {"--"; break}
                "" {"--"; break}
                default {[math]::Round($r.entered_score,2); break}
            }
            $values = 
            $r.id,
            $r.grade,
            $r.score,
            $r.submitted_at,
            $r.assignment_id,
            $r.user_id,
            $r.workflow_state,
            $r.grade_matches_current_submission,
            $r.graded_at,
            $r.grader_id,
            $r.excused,
            $r.late,
            $r.missing,
            $r.entered_grade,
            $scrubbed_score,
            $r.preview_url
            $values_string = $values -join "','"
            $records_string += "('" + $values_string + "')`r`n"
        }
    }

    $column_names = " (id,grade,score,submitted_at,assignment_id,user_id,workflow_state,grade_matches_current_submission,graded_at,grader_id,excused,late,missing,entered_grade,entered_score,preview_url)"
    $commit = CommitTable -table_name $table_name -cnx_string $Env:PSQL_CONNECTION_STRING -column_names $column_names -rowval_array $records_string
}

#endregion

### region ## Log results.

### Reset mirror status so that we can save on mirror run times
$query_string=
"
    update canvas_currentyear.config
    set needs_mirror=false
    where table_name in ('users','observees','courses','sections','enrollments');
"
$store_event = ExecuteNonQuery -ConnectionString $Env:PSQL_CONNECTION_STRING -command_string $query_string

### report time span to console
$EndDate=(GET-DATE)
$timespan = NEW-TIMESPAN -Start $StartDate -End $EndDate
"Run time: " + $timespan.TotalMinutes + " minutes"

#endregion
