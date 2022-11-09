#region setup section

$StartDate=(GET-DATE)

Import-Module SqlServer

$error.Clear()
. .\local_env_variables.ps1
. .\sync_functions.ps1

### Set procedure variables
$jobname = "canvas_PS_sync"
$db_name = "canvas_currentyear"

###$PSDefaultParameterValues['Out-File:Encoding'] = 'utf8'

#endregion

#region enrollments.remove
Write-Host "Canvas REST calls: removing enrollments."
$query_string="select * from SYNC_enrollments_remove"
$firstelement, $mirror_status = MySQLExecuteQuery -query_string $query_string -db_name $db_name

$query="select * from SYNC_enrollments_remove"
$rows = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query -Username $Username -Password $Password
$rec_count = 0
$reccount_max = $rows.Count

#### Set mirror status for enrollments to true if query returns rows
MirrorStatusCheck $rows "enrollments" $server_name $db_name $Username $Password

foreach ($rw in $rows)
{
    $rec_count++
    if ($rec_count % 50 -eq 0)
    {
        Write-Host "Enumerating sections for enrollments, at record $rec_count of $reccount_max."
    }
    $body_json = ConvertTo-Json @{"task"="delete"}
    $api_url="https://setoncatholic.instructure.com/api/v1/courses/"+$rw.course_id+"/enrollments/"+$rw.enrollment_id
    $comment = "user_id="+$rw.user_id+", course_id="+$rw.course_id+", section_id="+$rw.section_id
    $content = CanvasRESTCall $api_url 'DELETE' $body_json "enrollments.remove" $comment
}
#endregion

#region sections.remove

Write-Host "Canvas REST calls: removing sections."
$query="select * from SYNC_sections_remove"
$rows = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query -Username $Username -Password $Password

#### Set mirror status for sections to true if query returns rows
MirrorStatusCheck $rows "sections" $server_name $db_name $Username $Password

foreach ($rw in $rows)
{

    $comment = "canvas_sectionid="+$rw.canvas_sectionid+", sis_course_id="+$rw.sis_course_id+", sis_section_id="+$rw.sis_section_id

    $api_url="https://setoncatholic.instructure.com/api/v1/sections/"+$rw.canvas_sectionid+"/enrollments?state[]=completed&state[]=active&state[]=inactive"
    $content = CanvasRESTCall $api_url 'GET' $body_json "sections.remove.enrollments.delete" $comment $jobname
    
    if($content -ne $null)
    {
        $body_json = ConvertTo-Json @{"task"="delete"}
        foreach ($en in $content)
        {
            if($en.role_id -ne 7)
            {
                $api_url = "https://setoncatholic.instructure.com/api/v1/courses/"+$en.course_id+"/enrollments/"+$en.id+"?task=delete"
                $content = CanvasRESTCall $api_url 'DELETE' $body_json "sections.remove.enrollments.delete" $comment $jobname
            }
        }
    }
    $api_url="https://setoncatholic.instructure.com/api/v1/sections/"+$rw.canvas_sectionid
    $body =
    @{
        "course_section"=
        @{
	        "sis_section_id"= "";   ##$rw.sis_section_id+"_DISABLED";
        };
    }
    $body_json = ConvertTo-Json $body

    $content = CanvasRESTCall $api_url 'PUT' $body_json "sections.remove.namechange" $comment $jobname

    if($content -ne $null) {$content = CanvasRESTCall $api_url 'DELETE' $body_json "sections.remove.delete" $comment $jobname}
    else
    {
        $errormsg = "Error deleting section. Check REST log. canvas_section_id=" + $rw.canvas_sectionid + " sis_course_id=" + $rw.sis_course_id
        Write-Error $errormsg -ErrorAction:SilentlyContinue
    }

}

#endregion

#region courses.remove

Write-Host "Canvas REST calls: removing courses."
$query="select * from SYNC_courses_remove"
$rows = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query -Username $Username -Password $Password

#### Set mirror status for courses to true if query returns rows
MirrorStatusCheck $rows "courses" $server_name $db_name $Username $Password

foreach ($rw in $rows)
{
    $body_json = "This is a course delete request for courseid=" + $rw.canvas_course_id
    $api_url="https://setoncatholic.instructure.com/api/v1/courses/"+$rw.canvas_course_id+"?event=delete"
    $comment = "canvas_long_name="+$rw.long_name+", sis_course_id="+$rw.canvas_course_id
    $content = CanvasRESTCall $api_url 'DELETE' $body_json "courses.remove" $comment $jobname
}

#endregion

#region users.remove
Write-Host "Canvas REST calls: removing users."

### select users that need to be disabled from seton-sql3 ####
$query="select * from [dbo].[SYNC_students_disable]"
$students = @{}
try
    {$students = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query -Username $Username -Password $Password}
catch
    {Write-Host $Error}

#### Set mirror status for users to true if query returns rows
MirrorStatusCheck $students "users" $server_name $db_name $Username $Password

### for each student, execute API calls to disable student login ###
$stucount=0

foreach ($stu in $students)
{
    ++$stucount
    $msg="Working on student "+$stucount+" of "+$students.Count+" total."
    Write-Host $msg
    $stu_canvas_id = $stu.canvas_id
    $body_json = $null
    $api_url="https://setoncatholic.instructure.com/api/v1/users/$stu_canvas_id/logins"
    $comment = "canavs_user_id="+$stu.canvas_id
    $logins = CanvasRESTCall $api_url 'GET' $body_json "logins.get" $comment $jobname

    foreach ($login in $logins)
    {
        $api_url="https://setoncatholic.instructure.com/api/v1/accounts/1/logins/"+$login.id+"?login[unique_id]=DISABLED_"+$login.unique_id
        $content = CanvasRESTCall $api_url 'PUT' $body_json "logins.disableloginid" $comment $jobname
    }
}

#endregion

#region users.add

Write-Host "Canvas REST calls: adding users."
$query_string="select * from SYNC_users_add"
$rows = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query_string -Username $Username -Password $Password

#### Set mirror status for users to true if query returns rows
MirrorStatusCheck $rows "users" $server_name $db_name $Username $Password

foreach ($rw in $rows)
{
    $fullname = $rw.first_name + " " + $rw.last_name
    $sortablename = $rw.last_name + ", " + $rw.first_name
    $body =
    @{
        "user"=
        @{
            "name"=$fullname;
	        "sortable_name"=$sortablename;
            "terms_of_use"=$true;
            "skip_registration"=$true;
        };
        "pseudonym"=
        @{
            "unique_id"=$rw.email;
            "sis_user_id"=$rw.sis_user_id;
            "send_confirmation"=$true;
            "authentication_provider_id"=$rw.authentication_provider_id;
        };
        "enable_sis_reactivation"=$true;
    }
    $body_json = ConvertTo-Json $body

    $api_url="https://setoncatholic.instructure.com/api/v1/accounts/1/users"
    $comment = "sis_user_id="+$rw.sis_user_id+", canvas_user_id="+$created_user.id+", name="+$created_user.sortable_name
    $content = CanvasRESTCall $api_url 'POST' $body_json "student.add" $comment $jobname
    
    if($content -ne $null)
    {
        ### Sanitize for apostrophes
        $content.name=$content.name -replace "'","''"
        $content.sortable_name=$content.sortable_name -replace "'","''"

        ### Insert new user into table
        $query_string= "insert into users values ('"+
                            $content.sis_user_id+"','"+
                            $content.login_id+"','"+
                            $content.full_name+"','"+
                            $content.sortable_name+"','"+
                            $content.id+"');"
        $storetime = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query_string -Username $Username -Password $Password -ErrorAction SilentlyContinue
    }
}
#endregion

#region course.add
Write-Host "Canvas REST calls: adding courses."
$query="select * from SYNC_courses_add"
$rows = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query -Username $Username -Password $Password

#### Set mirror status for courses to true if query returns rows
MirrorStatusCheck $rows "courses" $server_name $db_name $Username $Password

foreach ($rw in $rows)
{
    $body =
    @{
        "course"=
        @{
            "name"=$rw.long_name;
            "course_code"=$rw.short_name;
            "term_id"=$rw.canvas_term_id;
	        "sis_course_id"=$rw.sis_course_id;
        };
        "enable_sis_reactivation"=$true;
    }
    $body_json = ConvertTo-Json $body

    $api_url="https://setoncatholic.instructure.com/api/v1/accounts/"+$rw.canvas_account_id+"/courses"
    $comment = "course_id="+$rw.sis_course_id
    $content = CanvasRESTCall $api_url 'POST' $body_json "courses.add" $comment $jobname
    
    ### Insert new course into course table
    $query_string= "insert into courses values ('"+
                        $content.sis_course_id+"','"+
                        $content.course_code+"','"+
                        $content.name+"','"+
                        $content.account_id+"','"+
                        $content.enrollment_term_id+"','"+
                        $content.id+"','"+
                        $content.workflow_state+"');"
    $storetime = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query_string -Username $Username -Password $Password -ErrorAction SilentlyContinue
}
#endregion

#region sections.add
Write-Host "Canvas REST calls: adding sections."
$query="select * from SYNC_sections_add"
$rows = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query -Username $Username -Password $Password

#### Set mirror status for sections to true if query returns rows
MirrorStatusCheck $rows "sections" $server_name $db_name $Username $Password

foreach ($rw in $rows)
{
    $body =
    @{
        "course_section"=
        @{
            "name"=$rw.name;
	        "sis_section_id"=$rw.sis_section_id;
        };
        "enable_sis_reactivation"=$true;
    }
    $body_json = ConvertTo-Json $body

    $api_url="https://setoncatholic.instructure.com/api/v1/courses/"+$rw.canvas_courseid+"/sections"
    $comment = "section="+$rw.name+", course_id="+$rw.canvas_courseid+", section_id="+$rw.sis_section_id+", course_id="+$rw.sis_course_id
    $content = CanvasRESTCall $api_url 'POST' $body_json "sections.add" $comment $jobname

    ### Insert new section into sections table 
    if($content -ne $null) {       
        $query_string= "insert into sections values ('"+
                        $content.sis_section_id+"','"+
                        $content.sis_course_id+"','"+
                        $content.name+"','"+
                        $content.course_id+"','"+
                        $content.id+"','"+
                        $content.nonxlist_course_id+"');"
        $storetime = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query_string -Username $Username -Password $Password -ErrorAction SilentlyContinue
    }
}
#endregion

#region enrollments.add
Write-Host "Canvas REST calls: adding enrollments."
$query="select * from SYNC_enrollments_add"
$rows = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query -Username $Username -Password $Password
$rec_count = 0
$reccount_max = $rows.Count

#### Set mirror status for enrollments to true if query returns rows
MirrorStatusCheck $rows "enrollments" $server_name $db_name $Username $Password

foreach ($rw in $rows)
{
    $rec_count++
    if ($rec_count % 50 -eq 0)
    {
        Write-Host "Adding enrollments, at record $rec_count of $reccount_max."
    }

    switch($rw.role_id)
    {
        3 {$enrollment_type="StudentEnrollment"}
        4 {$enrollment_type="TeacherEnrollment"}
    }

    $body =
    @{
        "enrollment"=
        @{
            "user_id"=[int]$rw.user_id;
	        "type"=$enrollment_type;
	        "enrollment_state"="active";
	        "course_section_id"=[int]$rw.section_id
        }
    }
    $body_json = ConvertTo-Json $body

    $api_url="https://setoncatholic.instructure.com/api/v1/courses/"+$rw.course_id+"/enrollments"
    $comment = "user_id="+$rw.user_id+", course_id="+$rw.course_id+", section_id="+$rw.section_id
    $content = CanvasRESTCall $api_url 'POST' $body_json "enrollments.add" $comment $jobname
    
}
#endregion

#region parents.add

#######Add parent accounts first ######################
#######################################################
#######################################################

### Grab parents to add, from seton-sql3
$query="select * from SYNC_parents_add"
try
{$parents = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query -Username $Username -Password $Password}
catch
{Write-Host "Failed to grab sync_add_parents from $db_name"}

#### Set mirror status for users to true if query returns rows
MirrorStatusCheck $parents "users" $server_name $db_name $Username $Password

### Create each parent with NO sis_user_id

$parentids = @()
foreach ($par in $parents)
{
    ### set parameters for the body
    $body =
    @{
        "user" = @{
            "name"=$par.full_name;
            "terms_of_use"=$true;
            "skip_registration"=$true}
        "pseudonym" = @{
            "unique_id"=$par.PS_email;
            "password"="supersecretpasswordyoucannothaveyeah";
            "send_confirmation"=$false}
    }
    $body_json = ConvertTo-Json $body ### covnert to JSON so that Invoke-WebRequest works

    try
    {
        $api_url="https://setoncatholic.instructure.com/api/v1/accounts/1/users"
        $comment = "parent_name="+$par.full_name+", email="+$par.PS_email
        $parent = CanvasRESTCall $api_url 'POST' $body_json "parent.add.user" $comment $jobname
        $parentids += $parent.id ### add the Canvas userid so that it can be processed later
        $comment
    }
    catch
    {
        $writehost = "Failure: " + $comment
        Write-Host $writehost
    }

    ############################################################################################
    ### We then need to add the user we just created to our local seton-sql3 so
    ### we don't have to do a full sync and the parent gets picked up with the next query
    ############################################################################################
    
    if($parent.id)
    {
        $parname = $parent.name -replace "'","''"
        $parsortable = $parent.sortable_name -replace "'","''"
        $query="INSERT INTO users (sis_user_id,login_id,full_name,sortable_name,id)
            VALUES ('','"+$parent.login_id+"','"+$parname+"','"+$parsortable+"','"+$parent.id+"')"
        try
            {$parents = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query -Username $Username -Password $Password}
        catch
            {Write-Host "Failed to insert new parent."}
    }
    
}

#### Then we need to set default permissions for all notification preferences for each parent that was added

foreach ($parentid in $parentids)
{
    try
    {
        $comment = "parent_canvas_id="+$parentid

        ### Grab communication channel id
        $api_url="https://setoncatholic.instructure.com/api/v1/users/$parentid/communication_channels"
        $body_json = $null
        $channel = CanvasRESTCall $api_url 'GET' $body_json "parent.add.getchannel" $comment $jobname
        $channelid = $channel.id
        ### Create json to set default notifications for parents. Pull from template file.
        $body_json = "{""as_user_id"":"+$parentid+","+ (Get-Content .\templates\parent_notification_template.json) + "}"
        $api_url="https://setoncatholic.instructure.com/api/v1/users/self/communication_channels/$channelid/notification_preferences"
        $content = CanvasRESTCall $api_url 'PUT' $body_json "parent.add.setprefs" $comment $jobname
    }
    catch
    {
        $writehost = "Failed to notification preferences for parentid = " + $parentid.ToString()
        Write-Host $writehost
    }
    
    $writehost = "parent_canvas_id "+$parentid.ToString() + " account processed."
    Write-Host $writehost
}

#endregion

#region relationships.add

####### Create observee relationships #################
#######################################################
#######################################################

### Grab relationships to add, from seton-sql3
$query="select * from canvas_currentyear.dbo.SYNC_observees_add"
try
{$relationships = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query -Username $Username -Password $Password}
catch
{Write-Host "Failed to grab sync_add_observees from $db_name"}
$body_json=""

#### Set mirror status for observees to true if query returns rows
MirrorStatusCheck $relationships "observees" $server_name $db_name $Username $Password

foreach ($rel in $relationships)
{
    try
    {
        $relparent = $rel.parent_canvas_id
        $relstudent = $rel.student_canvas_id
        $api_url="https://setoncatholic.instructure.com/api/v1/users/"+$relparent+"/observees/"+$relstudent
        $comment = "parent canvas_id="+$relparent+", student canvas_id="+$relstudent
        $observee = CanvasRESTCall $api_url 'PUT' $body_json "relationship.add" $comment $jobname

        $writehost = "Parent $relparent relationship created for student $relstudent"
        Write-Host $writehost
    }
    catch
    {
            
        $writehost = "Failed to create relationship between parent " + $relparent.ToString() + " and student " + $relstudent.ToString()
        Write-Host $writehost
    }
}

#endregion

#region relationships.remove

####### Remove observee relationships #################
#######################################################
#######################################################

### Grab relationships to remove, from seton-sql3
$query="select * from canvas_currentyear.dbo.SYNC_observees_remove"
try
    {$relationships = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query -Username $Username -Password $Password}
catch
    {Write-Host "Failed grab sync_remove_observees from $db_name"}

#### Set mirror status for observees to true if query returns rows
MirrorStatusCheck $relationships "observees" $server_name $db_name $Username $Password

foreach ($rel in $relationships)
{
    try
    {
        $api_url="https://setoncatholic.instructure.com/api/v1/users/"+$rel.parent_canvas_id+"/observees/"+$rel.student_canvas_id
        $body_json = $null
        $comment = "parent canvas_id="+$rel.parent_canvas_id+", student canvas_id="+$rel.student_canvas_id
        $observee = CanvasRESTCall $api_url 'DELETE' $body_json "relationship.remove" $comment $jobname
        
        $writehost = "Parent " + $rel.parent_canvas_id + " and student " + $rel.student_canvas_id + " relationship was deleted."
        Write-Host $writehost
    }
    catch
    {
        $writehost = "Failed to remove relationship between parent " + $rel.parent_canvas_id + " and student " + $rel.student_canvas_id
        Write-Host $writehost
    }
}


#endregion

#region parents.remove

####### remove parent accounts ########################
#######################################################
#######################################################

### Grab parent accounts to remove, from seton-sql3
$query="select * from canvas_currentyear.dbo.SYNC_parents_remove"
try
    {$parents = Invoke-Sqlcmd -ServerInstance $server_name -database $db_name -query $query -Username $Username -Password $Password}
catch
    {Write-Host $error}

#### Set mirror status for users to true if query returns rows
MirrorStatusCheck $parents "users" $server_name $db_name $Username $Password

foreach ($par in $parents)
{
    try
    {
        $api_url="https://setoncatholic.instructure.com/api/v1/accounts/1/users/"+$par.parent_canvas_id
        $body_json = $null
        $comment = "parent canvas_id="+$par.parent_canvas_id
        $parent = CanvasRESTCall $api_url 'DELETE' $body_json "parent.remove" $comment $jobname

        $writehost = "User " + $par.parent_canvas_id.ToString() + " was deleted."
        Write-Host $writehost
    }
    catch
    {
        $writehost = "Failed to delete user " + $par.parent_canvas_id.ToString()
        Write-Host $writehost
    }
}

#endregion

$EndDate=(GET-DATE)
$timespan = NEW-TIMESPAN –Start $StartDate –End $EndDate
"Run time: " + $timespan.TotalMinutes + " minutes"

$errortostore = $Error -replace "'", "''"
$query = "insert into job_results values ('"+$EndDate+"','Canvas Sync',"+$timespan.TotalMinutes+","+$error.Count+",'"+$errortostore+"')"
$results = (Invoke-Sqlcmd -ServerInstance $server_name -database "job_logs" -query $query -Username $Username -Password $Password)
