from bugbug import bugzilla, db
import os

# Ensure snapshot is present
db.download(bugzilla.BUGS_DB)

# Path to cached dataset
print("Local cache path:", bugzilla.BUGS_DB)

# Peek at first bug
for bug in bugzilla.get_bugs():
    #print(bug.keys())
    print(bug["id"], bug["status"], bug["summary"], bug["comments"][0], bug["creator"], bug["creation_time"])
    break

temp = """
dict_keys(['history', 'comments', 'blocks', 'cf_status_thunderbird_esr115', 'version', 'resolution', 'votes', 'see_also', 'cf_accessibility_severity', 'cf_fx_iteration', 'cf_qa_whiteboard', 'creator_detail', 'last_change_time', 'summary', 'component', 'flags', 'cf_status_firefox_esr115', 'severity', 'is_open', 'id', 'creator', 'cf_tracking_firefox_esr115', 'cf_tracking_thunderbird_esr115', 'mentors', 'op_sys', 'cf_tracking_firefox123', 'cf_crash_signature', 'groups', 'product', 'cf_rank', 'cf_last_resolved', 'cf_status_firefox114', 'mentors_detail', 'assigned_to_detail', 'alias', 'dupe_of', 'whiteboard', 'type', 'is_confirmed', 'duplicates', 'cf_webcompat_priority', 'platform', 'is_creator_accessible', 'creation_time', 'cf_status_firefox124', 'cf_cab_review', 'depends_on', 'cf_tracking_firefox122', 'priority', 'keywords', 'classification', 'is_cc_accessible', 'cc_detail', 'filed_via', 'regressions', 'regressed_by', 'url', 'cf_user_story', 'cf_performance_impact', 'assigned_to', 'cf_has_str', 'qa_contact', 'cf_a11y_review_project_flag', 'cf_status_firefox122', 'cf_status_firefox123', 'cc', 'status', 'cf_tracking_firefox124', 'cf_tracking_firefox_relnote', 'cf_fx_points', 'comment_count', 'target_milestone', 'attachments'])
"""