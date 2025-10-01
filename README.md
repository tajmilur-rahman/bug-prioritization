# Bug Prioritization

## Data Retrieval 
1. From root of project repository, run scripts/getdataset.py. 
This will retrieve the dataset of Bugbug and store as data/bugs.json. However this file is large (~ 8GB) and contains many unused data, we filter out the data which are useful for bug prioritization in the next step.
2. Get the right objects "bugs" from bugs.json file and remove unused but large fields like "history", "attachements", so that the data file size is reduced to ~ 6GB. The output file is bugs.ndjson which is more easy to process than .json file so the preprocessing steps can be done quickly on local machine.  
From root of project repository, run below command. Output is bugs.ndjson file (stored at https://drive.google.com/file/d/1983RBoiLobw2ITjvED6p8tmE_jh7fNB0/view?usp=share_link)

jq -c '
  ( if type == "array"                          then .[]
    elif type == "object" and has("bugs")       then .bugs[]
    elif type == "object" and has("result") and (.result|has("bugs")) then .result.bugs[]
    else . end )
  | select(type == "object")
  | del(.history?, .attachments?)
' data/bugs.json > data/bugs.ndjson


## Data Preprocessing
1. "process_bugs.py" cleans the data and stores the cleaned data in multiple .csv files inside "clean" folder.

Data fields after cleaning to be used for models training

Core text
•	summary
•	description (from comments[0].text if present)
Metadata
•	priority, severity, status, resolution, is_open, type, product, component, version, platform, op_sys, classification
•	People/fields: creator, assigned_to, qa_contact
•	Dates: creation_time, last_change_time, cf_last_resolved
•	Other: target_milestone, keywords (joined by ;), url, whiteboard, alias, dupe_of
Counts
•	Lists: duplicates_count, depends_on_count, blocks_count, cc_count, attachments_count, regressions_count, regressed_by_count, see_also_count
•	comment_count, votes
Text features
•	summary_len, desc_len, url_count, code_fence_count, has_stacktrace (heuristics on summary+description)
Custom fields (scalar subset)
•	cf_crash_signature, cf_rank, cf_webcompat_priority, cf_fx_points, cf_performance_impact, cf_user_story, cf_has_str, cf_qa_whiteboard, cf_fx_iteration, cf_cab_review, cf_accessibility_severity, cf_a11y_review_project_flag
Derived
•	days_open_est (from creation_time → last_change_time if both parse)


2. "merge_resolved.py" takes only the "resolved" bugs from all the cleaned .csv files and merge to one file for using in later steps. There were 563547 resolved bugs merged in to data/bugs_resolved.csv file. The same named file on reporsitory is a little sample of that.


