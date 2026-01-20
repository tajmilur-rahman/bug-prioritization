# Convert raw JSON data downloaded from bugbug's bugzilla into NDJSON file for later easier processing


jq -c '
  ( if type == "array"                          then .[]
    elif type == "object" and has("bugs")       then .bugs[]
    elif type == "object" and has("result") and (.result|has("bugs")) then .result.bugs[]
    else . end )
  | select(type == "object")
  | del(.history?, .attachments?)
' data/bugs.json > data/bugs.ndjson

# 1. Print the first object from bugs.ndjson
head -n 1 data/bugs.ndjson

# # Python code to extract field names of the bugs dataset
# # 2. Deserialize the JSON string into a Python dictionary
# import json
# json_string = "" # input with the first object json string
# data_dict = json.loads(json_string)

# # 3. Get the keys from the dictionary
# keys = data_dict.keys()

# # Print the keys (outputs a dict_keys object)
# print(keys)
# #Output: dict_keys(['history', 'comments', 'blocks', 'cf_status_thunderbird_esr115', 'version', 'resolution', 'votes', 'see_also', 'cf_accessibility_severity', 'cf_fx_iteration', 'cf_qa_whiteboard', 'creator_detail', 'last_change_time', 'summary', 'component', 'flags', 'cf_status_firefox_esr115', 'severity', 'is_open', 'id', 'creator', 'cf_tracking_firefox_esr115', 'cf_tracking_thunderbird_esr115', 'mentors', 'op_sys', 'cf_tracking_firefox123', 'cf_crash_signature', 'groups', 'product', 'cf_rank', 'cf_last_resolved', 'cf_status_firefox114', 'mentors_detail', 'assigned_to_detail', 'alias', 'dupe_of', 'whiteboard', 'type', 'is_confirmed', 'duplicates', 'cf_webcompat_priority', 'platform', 'is_creator_accessible', 'creation_time', 'cf_status_firefox124', 'cf_cab_review', 'depends_on', 'cf_tracking_firefox122', 'priority', 'keywords', 'classification', 'is_cc_accessible', 'cc_detail', 'filed_via', 'regressions', 'regressed_by', 'url', 'cf_user_story', 'cf_performance_impact', 'assigned_to', 'cf_has_str', 'qa_contact', 'cf_a11y_review_project_flag', 'cf_status_firefox122', 'cf_status_firefox123', 'cc', 'status', 'cf_tracking_firefox124', 'cf_tracking_firefox_relnote', 'cf_fx_points', 'comment_count', 'target_milestone', 'attachments'])

# # Convert to a list for standard list operations
# keys_list = list(keys)