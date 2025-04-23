# onet_dataset_29.2 Database Schema

This document describes the schema of the onet_dataset_29.2 database.

## Tables

- [abilities](#abilities)
- [abilities_to_work_activities](#abilities_to_work_activities)
- [abilities_to_work_context](#abilities_to_work_context)
- [alternate_titles](#alternate_titles)
- [basic_interests_to_riasec](#basic_interests_to_riasec)
- [content_model_reference](#content_model_reference)
- [dwa_reference](#dwa_reference)
- [education_training_and_experience](#education_training_and_experience)
- [education_training_and_experience_categories](#education_training_and_experience_categories)
- [emerging_tasks](#emerging_tasks)
- [interests](#interests)
- [interests_illustrative_activities](#interests_illustrative_activities)
- [interests_illustrative_occupations](#interests_illustrative_occupations)
- [iwa_reference](#iwa_reference)
- [job_zone_reference](#job_zone_reference)
- [job_zones](#job_zones)
- [knowledge](#knowledge)
- [level_scale_anchors](#level_scale_anchors)
- [occupation_data](#occupation_data)
- [occupation_knowledge](#occupation_knowledge)
- [occupation_level_metadata](#occupation_level_metadata)
- [occupation_skills](#occupation_skills)
- [occupation_work_activities](#occupation_work_activities)
- [onet_metadata](#onet_metadata)
- [related_occupations](#related_occupations)
- [riasec_keywords](#riasec_keywords)
- [sample_of_reported_titles](#sample_of_reported_titles)
- [scales_reference](#scales_reference)
- [skills](#skills)
- [skills_to_work_activities](#skills_to_work_activities)
- [skills_to_work_context](#skills_to_work_context)
- [survey_booklet_locations](#survey_booklet_locations)
- [task_categories](#task_categories)
- [task_ratings](#task_ratings)
- [task_statements](#task_statements)
- [tasks_to_dwas](#tasks_to_dwas)
- [technology_skills](#technology_skills)
- [tools_used](#tools_used)
- [unspsc_reference](#unspsc_reference)
- [work_activities](#work_activities)
- [work_context](#work_context)
- [work_context_categories](#work_context_categories)
- [work_styles](#work_styles)
- [work_values](#work_values)

## abilities

This table contains 91,416 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| onetsoc_code | VARCHAR | |
| title | VARCHAR | |
| element_id | VARCHAR | |
| element_name | VARCHAR | |
| scale_id | VARCHAR | |
| scale_name | VARCHAR | |
| data_value | DOUBLE | |
| n | BIGINT | |
| standard_error | DOUBLE | |
| lower_ci_bound | DOUBLE | |
| upper_ci_bound | DOUBLE | |
| recommend_suppress | VARCHAR | |
| not_relevant | VARCHAR | |
| date | VARCHAR | |
| domain_source | VARCHAR | |

### Sample Data

| onetsoc_code | title | element_id | element_name | scale_id | scale_name | data_value | n | standard_error | lower_ci_bound | upper_ci_bound | recommend_suppress | not_relevant | date | domain_source |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 11-1011.00 | Chief Executives | 1.A.1.a.1 | Oral Comprehension | IM | Importance | 4.62 | 8 | 0.183 | 4.2664 | 4.9836 | N |  | 08/2023 | Analyst |
| 11-1011.00 | Chief Executives | 1.A.1.a.1 | Oral Comprehension | LV | Level | 4.88 | 8 | 0.125 | 4.63 | 5.12 | N | N | 08/2023 | Analyst |
| 11-1011.00 | Chief Executives | 1.A.1.a.2 | Written Comprehension | IM | Importance | 4.25 | 8 | 0.1637 | 3.9292 | 4.5708 | N |  | 08/2023 | Analyst |

---

## abilities_to_work_activities

This table contains 381 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| abilities_element_id | VARCHAR | |
| abilities_element_name | VARCHAR | |
| work_activities_element_id | VARCHAR | |
| work_activities_element_name | VARCHAR | |

### Sample Data

| abilities_element_id | abilities_element_name | work_activities_element_id | work_activities_element_name |
| --- | --- | --- | --- |
| 1.A.1.a.1 | Oral Comprehension | 4.A.1.a.1 | Getting Information |
| 1.A.1.a.1 | Oral Comprehension | 4.A.1.a.2 | Monitoring Processes, Materials, or Surroundings |
| 1.A.1.a.1 | Oral Comprehension | 4.A.1.b.1 | Identifying Objects, Actions, and Events |

---

## abilities_to_work_context

This table contains 139 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| abilities_element_id | VARCHAR | |
| abilities_element_name | VARCHAR | |
| work_context_element_id | VARCHAR | |
| work_context_element_name | VARCHAR | |

### Sample Data

| abilities_element_id | abilities_element_name | work_context_element_id | work_context_element_name |
| --- | --- | --- | --- |
| 1.A.1.a.1 | Oral Comprehension | 4.C.1.a.2.c | Public Speaking |
| 1.A.1.a.1 | Oral Comprehension | 4.C.1.a.2.f | Telephone Conversations |
| 1.A.1.a.1 | Oral Comprehension | 4.C.1.a.2.l | Face-to-Face Discussions with Individuals and W... |

---

## alternate_titles

This table contains 56,560 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| onetsoc_code | VARCHAR | |
| title | VARCHAR | |
| alternate_title | VARCHAR | |
| short_title | VARCHAR | |
| sources | VARCHAR | |

### Sample Data

| onetsoc_code | title | alternate_title | short_title | sources |
| --- | --- | --- | --- | --- |
| 11-1011.00 | Chief Executives | Aeronautics Commission Director |  | 08 |
| 11-1011.00 | Chief Executives | Agency Owner |  | 10 |
| 11-1011.00 | Chief Executives | Agricultural Services Director |  | 08 |

---

## basic_interests_to_riasec

This table contains 53 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| basic_interests_element_id | VARCHAR | |
| basic_interests_element_name | VARCHAR | |
| riasec_element_id | VARCHAR | |
| riasec_element_name | VARCHAR | |

### Sample Data

| basic_interests_element_id | basic_interests_element_name | riasec_element_id | riasec_element_name |
| --- | --- | --- | --- |
| 1.B.3.a | Mechanics/Electronics | 1.B.1.a | Realistic |
| 1.B.3.b | Construction/Woodwork | 1.B.1.a | Realistic |
| 1.B.3.c | Transportation/Machine Operation | 1.B.1.a | Realistic |

---

## content_model_reference

This table contains 627 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| element_id | VARCHAR | |
| element_name | VARCHAR | |
| description | VARCHAR | |

### Sample Data

| element_id | element_name | description |
| --- | --- | --- |
| 1 | Worker Characteristics | Worker Characteristics |
| 1.A | Abilities | Enduring attributes of the individual that infl... |
| 1.A.1 | Cognitive Abilities | Abilities that influence the acquisition and ap... |

---

## dwa_reference

This table contains 2,087 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| element_id | VARCHAR | |
| element_name | VARCHAR | |
| iwa_id | VARCHAR | |
| iwa_title | VARCHAR | |
| dwa_id | VARCHAR | |
| dwa_title | VARCHAR | |

### Sample Data

| element_id | element_name | iwa_id | iwa_title | dwa_id | dwa_title |
| --- | --- | --- | --- | --- | --- |
| 4.A.1.a.1 | Getting Information | 4.A.1.a.1.I01 | Study details of artistic productions. | 4.A.1.a.1.I01.D01 | Review art or design materials. |
| 4.A.1.a.1 | Getting Information | 4.A.1.a.1.I01 | Study details of artistic productions. | 4.A.1.a.1.I01.D02 | Study details of musical compositions. |
| 4.A.1.a.1 | Getting Information | 4.A.1.a.1.I01 | Study details of artistic productions. | 4.A.1.a.1.I01.D03 | Review production information to determine cost... |

---

## education_training_and_experience

This table contains 36,209 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| onetsoc_code | VARCHAR | |
| title | VARCHAR | |
| element_id | VARCHAR | |
| element_name | VARCHAR | |
| scale_id | VARCHAR | |
| scale_name | VARCHAR | |
| category | DOUBLE | |
| data_value | DOUBLE | |
| n | BIGINT | |
| standard_error | DOUBLE | |
| lower_ci_bound | DOUBLE | |
| upper_ci_bound | DOUBLE | |
| recommend_suppress | VARCHAR | |
| date | VARCHAR | |
| domain_source | VARCHAR | |

### Sample Data

| onetsoc_code | title | element_id | element_name | scale_id | scale_name | category | data_value | n | standard_error | lower_ci_bound | upper_ci_bound | recommend_suppress | date | domain_source |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 11-1011.00 | Chief Executives | 2.D.1 | Required Level of Education | RL | Required Level Of Education (Categories 1-12) | 1.0 | 0.0 | 28 | 0.0 |  |  | N | 08/2023 | Incumbent |
| 11-1011.00 | Chief Executives | 2.D.1 | Required Level of Education | RL | Required Level Of Education (Categories 1-12) | 2.0 | 4.46 | 28 | 4.1428 | 0.6307 | 25.5524 | N | 08/2023 | Incumbent |
| 11-1011.00 | Chief Executives | 2.D.1 | Required Level of Education | RL | Required Level Of Education (Categories 1-12) | 3.0 | 0.0 | 28 | 0.0 |  |  | N | 08/2023 | Incumbent |

---

## education_training_and_experience_categories

This table contains 41 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| element_id | VARCHAR | |
| element_name | VARCHAR | |
| scale_id | VARCHAR | |
| scale_name | VARCHAR | |
| category | BIGINT | |
| category_description | VARCHAR | |

### Sample Data

| element_id | element_name | scale_id | scale_name | category | category_description |
| --- | --- | --- | --- | --- | --- |
| 2.D.1 | Required Level of Education | RL | Required Level Of Education (Categories 1-12) | 1 | Less than a High School Diploma |
| 2.D.1 | Required Level of Education | RL | Required Level Of Education (Categories 1-12) | 2 | High School Diploma - or the equivalent (for ex... |
| 2.D.1 | Required Level of Education | RL | Required Level Of Education (Categories 1-12) | 3 | Post-Secondary Certificate - awarded for traini... |

---

## emerging_tasks

This table contains 240 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| onetsoc_code | VARCHAR | |
| title | VARCHAR | |
| task | VARCHAR | |
| category | VARCHAR | |
| original_task_id | DOUBLE | |
| original_task | VARCHAR | |
| date | VARCHAR | |
| domain_source | VARCHAR | |

### Sample Data

| onetsoc_code | title | task | category | original_task_id | original_task | date | domain_source |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 11-3013.00 | Facilities Managers | Plan, create, and manage budgets for contracts,... | Revision | 21280.0 | Plan, administer, and control budgets for contr... | 08/2024 | Incumbent |
| 11-3013.00 | Facilities Managers | Review and approve payroll for employees. | New |  |  | 08/2024 | Incumbent |
| 11-3051.02 | Geothermal Production Managers | Conduct employee safety training. | New |  |  | 07/2011 | Incumbent |

---

## interests

This table contains 8,307 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| onetsoc_code | VARCHAR | |
| title | VARCHAR | |
| element_id | VARCHAR | |
| element_name | VARCHAR | |
| scale_id | VARCHAR | |
| scale_name | VARCHAR | |
| data_value | DOUBLE | |
| date | VARCHAR | |
| domain_source | VARCHAR | |

### Sample Data

| onetsoc_code | title | element_id | element_name | scale_id | scale_name | data_value | date | domain_source |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 11-1011.00 | Chief Executives | 1.B.1.a | Realistic | OI | Occupational Interests | 1.3 | 11/2023 | Machine Learning |
| 11-1011.00 | Chief Executives | 1.B.1.b | Investigative | OI | Occupational Interests | 3.24 | 11/2023 | Machine Learning |
| 11-1011.00 | Chief Executives | 1.B.1.c | Artistic | OI | Occupational Interests | 2.08 | 11/2023 | Machine Learning |

---

## interests_illustrative_activities

This table contains 188 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| element_id | VARCHAR | |
| element_name | VARCHAR | |
| interest_type | VARCHAR | |
| activity | VARCHAR | |

### Sample Data

| element_id | element_name | interest_type | activity |
| --- | --- | --- | --- |
| 1.B.1.a | Realistic | General | Build kitchen cabinets. |
| 1.B.1.a | Realistic | General | Drive a truck to deliver packages to offices an... |
| 1.B.1.a | Realistic | General | Put out forest fires. |

---

## interests_illustrative_occupations

This table contains 186 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| element_id | VARCHAR | |
| element_name | VARCHAR | |
| interest_type | VARCHAR | |
| onetsoc_code | VARCHAR | |
| title | VARCHAR | |

### Sample Data

| element_id | element_name | interest_type | onetsoc_code | title |
| --- | --- | --- | --- | --- |
| 1.B.1.a | Realistic | General | 17-3024.01 | Robotics Technicians |
| 1.B.1.a | Realistic | General | 45-2091.00 | Agricultural Equipment Operators |
| 1.B.1.a | Realistic | General | 47-2031.00 | Carpenters |

---

## iwa_reference

This table contains 332 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| element_id | VARCHAR | |
| element_name | VARCHAR | |
| iwa_id | VARCHAR | |
| iwa_title | VARCHAR | |

### Sample Data

| element_id | element_name | iwa_id | iwa_title |
| --- | --- | --- | --- |
| 4.A.1.a.1 | Getting Information | 4.A.1.a.1.I01 | Study details of artistic productions. |
| 4.A.1.a.1 | Getting Information | 4.A.1.a.1.I02 | Read documents or materials to inform work proc... |
| 4.A.1.a.1 | Getting Information | 4.A.1.a.1.I03 | Investigate criminal or legal matters. |

---

## job_zone_reference

This table contains 5 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| job_zone | BIGINT | |
| name | VARCHAR | |
| experience | VARCHAR | |
| education | VARCHAR | |
| job_training | VARCHAR | |
| examples | VARCHAR | |
| svp_range | VARCHAR | |

### Sample Data

| job_zone | name | experience | education | job_training | examples | svp_range |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | Job Zone One: Little or No Preparation Needed | Little or no previous work-related skill, knowl... | Some of these occupations may require a high sc... | Employees in these occupations need anywhere fr... | These occupations involve following instruction... | (Below 4.0) |
| 2 | Job Zone Two: Some Preparation Needed | Some previous work-related skill, knowledge, or... | These occupations usually require a high school... | Employees in these occupations need anywhere fr... | These occupations often involve using your know... | (4.0 to < 6.0) |
| 3 | Job Zone Three: Medium Preparation Needed | Previous work-related skill, knowledge, or expe... | Most occupations in this zone require training ... | Employees in these occupations usually need one... | These occupations usually involve using communi... | (6.0 to < 7.0) |

---

## job_zones

This table contains 923 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| onetsoc_code | VARCHAR | |
| title | VARCHAR | |
| job_zone | BIGINT | |
| date | VARCHAR | |
| domain_source | VARCHAR | |

### Sample Data

| onetsoc_code | title | job_zone | date | domain_source |
| --- | --- | --- | --- | --- |
| 11-1011.00 | Chief Executives | 5 | 08/2023 | Analyst |
| 11-1011.03 | Chief Sustainability Officers | 5 | 08/2021 | Analyst |
| 11-1021.00 | General and Operations Managers | 4 | 08/2023 | Analyst |

---

## knowledge

This table contains 58,014 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| onetsoc_code | VARCHAR | |
| title | VARCHAR | |
| element_id | VARCHAR | |
| element_name | VARCHAR | |
| scale_id | VARCHAR | |
| scale_name | VARCHAR | |
| data_value | DOUBLE | |
| n | DOUBLE | |
| standard_error | DOUBLE | |
| lower_ci_bound | DOUBLE | |
| upper_ci_bound | DOUBLE | |
| recommend_suppress | VARCHAR | |
| not_relevant | VARCHAR | |
| date | VARCHAR | |
| domain_source | VARCHAR | |

### Sample Data

| onetsoc_code | title | element_id | element_name | scale_id | scale_name | data_value | n | standard_error | lower_ci_bound | upper_ci_bound | recommend_suppress | not_relevant | date | domain_source |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 11-1011.00 | Chief Executives | 2.C.1.a | Administration and Management | IM | Importance | 4.78 | 28.0 | 0.1102 | 4.5564 | 5.0 | N |  | 08/2023 | Incumbent |
| 11-1011.00 | Chief Executives | 2.C.1.a | Administration and Management | LV | Level | 6.5 | 28.0 | 0.213 | 6.0666 | 6.9409 | N | N | 08/2023 | Incumbent |
| 11-1011.00 | Chief Executives | 2.C.1.b | Administrative | IM | Importance | 2.42 | 28.0 | 0.4651 | 1.4662 | 3.3749 | N |  | 08/2023 | Incumbent |

---

## level_scale_anchors

This table contains 483 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| element_id | VARCHAR | |
| element_name | VARCHAR | |
| scale_id | VARCHAR | |
| scale_name | VARCHAR | |
| anchor_value | BIGINT | |
| anchor_description | VARCHAR | |

### Sample Data

| element_id | element_name | scale_id | scale_name | anchor_value | anchor_description |
| --- | --- | --- | --- | --- | --- |
| 1.A.1.a.1 | Oral Comprehension | LV | Level | 2 | Understand a television commercial |
| 1.A.1.a.1 | Oral Comprehension | LV | Level | 4 | Understand a coach's oral instructions for a sport |
| 1.A.1.a.1 | Oral Comprehension | LV | Level | 6 | Understand a lecture on advanced physics |

---

## occupation_data

This table contains 1,016 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| onetsoc_code | VARCHAR | |
| title | VARCHAR | |
| description | VARCHAR | |

### Sample Data

| onetsoc_code | title | description |
| --- | --- | --- |
| 11-1011.00 | Chief Executives | Determine and formulate policies and provide ov... |
| 11-1011.03 | Chief Sustainability Officers | Communicate and coordinate with management, sha... |
| 11-1021.00 | General and Operations Managers | Plan, direct, or coordinate the operations of p... |

---

## occupation_knowledge

This table contains 58,014 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| onetsoc_code | VARCHAR | |
| title | VARCHAR | |
| knowledge_area | VARCHAR | |
| scale_id | VARCHAR | |
| knowledge_level | DOUBLE | |
| standard_error | DOUBLE | |
| lower_ci_bound | DOUBLE | |
| upper_ci_bound | DOUBLE | |
| recommend_suppress | VARCHAR | |
| not_relevant | VARCHAR | |

### Sample Data

| onetsoc_code | title | knowledge_area | scale_id | knowledge_level | standard_error | lower_ci_bound | upper_ci_bound | recommend_suppress | not_relevant |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 11-1011.00 | Chief Executives | Administration and Management | IM | 4.78 | 0.1102 | 4.5564 | 5.0 | N |  |
| 11-1011.00 | Chief Executives | Administration and Management | LV | 6.5 | 0.213 | 6.0666 | 6.9409 | N | N |
| 11-1011.00 | Chief Executives | Administrative | IM | 2.42 | 0.4651 | 1.4662 | 3.3749 | N |  |

---

## occupation_level_metadata

This table contains 31,776 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| onetsoc_code | VARCHAR | |
| title | VARCHAR | |
| item | VARCHAR | |
| response | VARCHAR | |
| n | DOUBLE | |
| percent | DOUBLE | |
| date | VARCHAR | |

### Sample Data

| onetsoc_code | title | item | response | n | percent | date |
| --- | --- | --- | --- | --- | --- | --- |
| 11-1011.00 | Chief Executives | Data Collection Mode | Paper | 95.0 | 18.9 | 08/2023 |
| 11-1011.00 | Chief Executives | Data Collection Mode | Web | 95.0 | 81.1 | 08/2023 |
| 11-1011.00 | Chief Executives | Employee Completeness Rate |  |  | 89.6 | 08/2023 |

---

## occupation_skills

This table contains 61,530 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| onetsoc_code | VARCHAR | |
| title | VARCHAR | |
| skill_name | VARCHAR | |
| scale_id | VARCHAR | |
| skill_level | DOUBLE | |
| standard_error | DOUBLE | |
| lower_ci_bound | DOUBLE | |
| upper_ci_bound | DOUBLE | |
| recommend_suppress | VARCHAR | |
| not_relevant | VARCHAR | |

### Sample Data

| onetsoc_code | title | skill_name | scale_id | skill_level | standard_error | lower_ci_bound | upper_ci_bound | recommend_suppress | not_relevant |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 11-1011.00 | Chief Executives | Reading Comprehension | IM | 4.12 | 0.125 | 3.88 | 4.37 | N |  |
| 11-1011.00 | Chief Executives | Reading Comprehension | LV | 4.62 | 0.183 | 4.2664 | 4.9836 | N | N |
| 11-1011.00 | Chief Executives | Active Listening | IM | 4.0 | 0.0 | 4.0 | 4.0 | N |  |

---

## occupation_work_activities

This table contains 72,078 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| onetsoc_code | VARCHAR | |
| title | VARCHAR | |
| activity | VARCHAR | |
| scale_id | VARCHAR | |
| activity_level | DOUBLE | |
| standard_error | DOUBLE | |
| lower_ci_bound | DOUBLE | |
| upper_ci_bound | DOUBLE | |
| recommend_suppress | VARCHAR | |
| not_relevant | VARCHAR | |

### Sample Data

| onetsoc_code | title | activity | scale_id | activity_level | standard_error | lower_ci_bound | upper_ci_bound | recommend_suppress | not_relevant |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 11-1011.00 | Chief Executives | Getting Information | IM | 4.56 | 0.1559 | 4.2369 | 4.8756 | N |  |
| 11-1011.00 | Chief Executives | Getting Information | LV | 4.89 | 0.1727 | 4.5393 | 5.2458 | N | N |
| 11-1011.00 | Chief Executives | Monitoring Processes, Materials, or Surroundings | IM | 4.25 | 0.2125 | 3.813 | 4.6823 | N |  |

---

## onet_metadata

This table contains 40 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| file_name | VARCHAR | |
| table_name | VARCHAR | |
| original_columns | VARCHAR | |
| row_count | INTEGER | |
| import_date | TIMESTAMP | |

### Sample Data

| file_name | table_name | original_columns | row_count | import_date |
| --- | --- | --- | --- | --- |
| Abilities to Work Activities.xlsx | abilities_to_work_activities | Abilities Element ID, Abilities Element Name, W... | 381 | 2025-04-23 11:27:13.760000 |
| Abilities.xlsx | abilities | O*NET-SOC Code, Title, Element ID, Element Name... | 91416 | 2025-04-23 11:27:20.023000 |
| IWA Reference.xlsx | iwa_reference | Element ID, Element Name, IWA ID, IWA Title | 332 | 2025-04-23 11:27:20.048000 |

---

## related_occupations

This table contains 18,460 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| onetsoc_code | VARCHAR | |
| title | VARCHAR | |
| related_onetsoc_code | VARCHAR | |
| related_title | VARCHAR | |
| relatedness_tier | VARCHAR | |
| index | BIGINT | |

### Sample Data

| onetsoc_code | title | related_onetsoc_code | related_title | relatedness_tier | index |
| --- | --- | --- | --- | --- | --- |
| 11-1011.00 | Chief Executives | 11-1021.00 | General and Operations Managers | Primary-Short | 1 |
| 11-1011.00 | Chief Executives | 11-2032.00 | Public Relations Managers | Primary-Short | 2 |
| 11-1011.00 | Chief Executives | 11-9151.00 | Social and Community Service Managers | Primary-Short | 3 |

---

## riasec_keywords

This table contains 75 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| element_id | VARCHAR | |
| element_name | VARCHAR | |
| keyword | VARCHAR | |
| keyword_type | VARCHAR | |

### Sample Data

| element_id | element_name | keyword | keyword_type |
| --- | --- | --- | --- |
| 1.B.1.a | Realistic | Build | Action |
| 1.B.1.a | Realistic | Drive | Action |
| 1.B.1.a | Realistic | Install | Action |

---

## sample_of_reported_titles

This table contains 7,796 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| onetsoc_code | VARCHAR | |
| title | VARCHAR | |
| reported_job_title | VARCHAR | |
| shown_in_my_next_move | VARCHAR | |

### Sample Data

| onetsoc_code | title | reported_job_title | shown_in_my_next_move |
| --- | --- | --- | --- |
| 11-1011.00 | Chief Executives | CEO (Chief Executive Officer) | Y |
| 11-1011.00 | Chief Executives | Chief Diversity Officer (CDO) | N |
| 11-1011.00 | Chief Executives | Chief Financial Officer (CFO) | Y |

---

## scales_reference

This table contains 29 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| scale_id | VARCHAR | |
| scale_name | VARCHAR | |
| minimum | BIGINT | |
| maximum | BIGINT | |

### Sample Data

| scale_id | scale_name | minimum | maximum |
| --- | --- | --- | --- |
| AO | Automation | 1 | 5 |
| CF | Frequency | 1 | 5 |
| CN | Amount of Contact | 1 | 5 |

---

## skills

This table contains 61,530 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| onetsoc_code | VARCHAR | |
| title | VARCHAR | |
| element_id | VARCHAR | |
| element_name | VARCHAR | |
| scale_id | VARCHAR | |
| scale_name | VARCHAR | |
| data_value | DOUBLE | |
| n | BIGINT | |
| standard_error | DOUBLE | |
| lower_ci_bound | DOUBLE | |
| upper_ci_bound | DOUBLE | |
| recommend_suppress | VARCHAR | |
| not_relevant | VARCHAR | |
| date | VARCHAR | |
| domain_source | VARCHAR | |

### Sample Data

| onetsoc_code | title | element_id | element_name | scale_id | scale_name | data_value | n | standard_error | lower_ci_bound | upper_ci_bound | recommend_suppress | not_relevant | date | domain_source |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 11-1011.00 | Chief Executives | 2.A.1.a | Reading Comprehension | IM | Importance | 4.12 | 8 | 0.125 | 3.88 | 4.37 | N |  | 08/2023 | Analyst |
| 11-1011.00 | Chief Executives | 2.A.1.a | Reading Comprehension | LV | Level | 4.62 | 8 | 0.183 | 4.2664 | 4.9836 | N | N | 08/2023 | Analyst |
| 11-1011.00 | Chief Executives | 2.A.1.b | Active Listening | IM | Importance | 4.0 | 8 | 0.0 | 4.0 | 4.0 | N |  | 08/2023 | Analyst |

---

## skills_to_work_activities

This table contains 232 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| skills_element_id | VARCHAR | |
| skills_element_name | VARCHAR | |
| work_activities_element_id | VARCHAR | |
| work_activities_element_name | VARCHAR | |

### Sample Data

| skills_element_id | skills_element_name | work_activities_element_id | work_activities_element_name |
| --- | --- | --- | --- |
| 2.A.1.a | Reading Comprehension | 4.A.1.a.1 | Getting Information |
| 2.A.1.a | Reading Comprehension | 4.A.1.a.2 | Monitoring Processes, Materials, or Surroundings |
| 2.A.1.a | Reading Comprehension | 4.A.1.b.1 | Identifying Objects, Actions, and Events |

---

## skills_to_work_context

This table contains 96 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| skills_element_id | VARCHAR | |
| skills_element_name | VARCHAR | |
| work_context_element_id | VARCHAR | |
| work_context_element_name | VARCHAR | |

### Sample Data

| skills_element_id | skills_element_name | work_context_element_id | work_context_element_name |
| --- | --- | --- | --- |
| 2.A.1.a | Reading Comprehension | 4.C.1.a.2.h | E-Mail |
| 2.A.1.b | Active Listening | 4.C.1.a.2.c | Public Speaking |
| 2.A.1.b | Active Listening | 4.C.1.a.2.f | Telephone Conversations |

---

## survey_booklet_locations

This table contains 227 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| element_id | VARCHAR | |
| element_name | VARCHAR | |
| survey_item_number | VARCHAR | |
| scale_id | VARCHAR | |
| scale_name | VARCHAR | |

### Sample Data

| element_id | element_name | survey_item_number | scale_id | scale_name |
| --- | --- | --- | --- | --- |
| 1.C.1.a | Achievement/Effort | KN39a | IM | Importance |
| 1.C.1.b | Persistence | KN39b | IM | Importance |
| 1.C.1.c | Initiative | KN39c | IM | Importance |

---

## task_categories

This table contains 7 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| scale_id | VARCHAR | |
| scale_name | VARCHAR | |
| category | BIGINT | |
| category_description | VARCHAR | |

### Sample Data

| scale_id | scale_name | category | category_description |
| --- | --- | --- | --- |
| FT | Frequency of Task (Categories 1-7) | 1 | Yearly or less |
| FT | Frequency of Task (Categories 1-7) | 2 | More than yearly |
| FT | Frequency of Task (Categories 1-7) | 3 | More than monthly |

---

## task_ratings

This table contains 158,751 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| onetsoc_code | VARCHAR | |
| title | VARCHAR | |
| task_id | BIGINT | |
| task | VARCHAR | |
| scale_id | VARCHAR | |
| scale_name | VARCHAR | |
| category | DOUBLE | |
| data_value | DOUBLE | |
| n | DOUBLE | |
| standard_error | DOUBLE | |
| lower_ci_bound | DOUBLE | |
| upper_ci_bound | DOUBLE | |
| recommend_suppress | VARCHAR | |
| date | VARCHAR | |
| domain_source | VARCHAR | |

### Sample Data

| onetsoc_code | title | task_id | task | scale_id | scale_name | category | data_value | n | standard_error | lower_ci_bound | upper_ci_bound | recommend_suppress | date | domain_source |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 11-1011.00 | Chief Executives | 8823 | Direct or coordinate an organization's financia... | FT | Frequency of Task (Categories 1-7) | 1.0 | 5.92 | 76.0 | 4.2651 | 1.3474 | 22.4442 | N | 08/2023 | Incumbent |
| 11-1011.00 | Chief Executives | 8823 | Direct or coordinate an organization's financia... | FT | Frequency of Task (Categories 1-7) | 2.0 | 15.98 | 76.0 | 5.6031 | 7.6477 | 30.3982 | N | 08/2023 | Incumbent |
| 11-1011.00 | Chief Executives | 8823 | Direct or coordinate an organization's financia... | FT | Frequency of Task (Categories 1-7) | 3.0 | 29.68 | 76.0 | 9.5346 | 14.518 | 51.1837 | N | 08/2023 | Incumbent |

---

## task_statements

This table contains 18,796 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| onetsoc_code | VARCHAR | |
| title | VARCHAR | |
| task_id | BIGINT | |
| task | VARCHAR | |
| task_type | VARCHAR | |
| incumbents_responding | DOUBLE | |
| date | VARCHAR | |
| domain_source | VARCHAR | |

### Sample Data

| onetsoc_code | title | task_id | task | task_type | incumbents_responding | date | domain_source |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 11-1011.00 | Chief Executives | 8823 | Direct or coordinate an organization's financia... | Core | 95.0 | 08/2023 | Incumbent |
| 11-1011.00 | Chief Executives | 8824 | Confer with board members, organization officia... | Core | 95.0 | 08/2023 | Incumbent |
| 11-1011.00 | Chief Executives | 8827 | Prepare budgets for approval, including those f... | Core | 95.0 | 08/2023 | Incumbent |

---

## tasks_to_dwas

This table contains 23,233 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| onetsoc_code | VARCHAR | |
| title | VARCHAR | |
| task_id | BIGINT | |
| task | VARCHAR | |
| dwa_id | VARCHAR | |
| dwa_title | VARCHAR | |
| date | VARCHAR | |
| domain_source | VARCHAR | |

### Sample Data

| onetsoc_code | title | task_id | task | dwa_id | dwa_title | date | domain_source |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 11-1011.00 | Chief Executives | 20461 | Review and analyze legislation, laws, or public... | 4.A.2.a.4.I09.D03 | Analyze impact of legal or regulatory changes. | 07/2014 | Analyst |
| 11-1011.00 | Chief Executives | 20461 | Review and analyze legislation, laws, or public... | 4.A.4.b.6.I08.D04 | Advise others on legal or regulatory compliance... | 07/2014 | Analyst |
| 11-1011.00 | Chief Executives | 8823 | Direct or coordinate an organization's financia... | 4.A.4.b.4.I09.D02 | Direct financial operations. | 03/2014 | Analyst |

---

## technology_skills

This table contains 32,627 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| onetsoc_code | VARCHAR | |
| title | VARCHAR | |
| example | VARCHAR | |
| commodity_code | BIGINT | |
| commodity_title | VARCHAR | |
| hot_technology | VARCHAR | |
| in_demand | VARCHAR | |

### Sample Data

| onetsoc_code | title | example | commodity_code | commodity_title | hot_technology | in_demand |
| --- | --- | --- | --- | --- | --- | --- |
| 11-1011.00 | Chief Executives | Adobe Acrobat | 43232202 | Document management software | Y | N |
| 11-1011.00 | Chief Executives | AdSense Tracker | 43232306 | Data base user interface and query software | N | N |
| 11-1011.00 | Chief Executives | Atlassian JIRA | 43232201 | Content workflow software | Y | N |

---

## tools_used

This table contains 41,662 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| onetsoc_code | VARCHAR | |
| title | VARCHAR | |
| example | VARCHAR | |
| commodity_code | BIGINT | |
| commodity_title | VARCHAR | |

### Sample Data

| onetsoc_code | title | example | commodity_code | commodity_title |
| --- | --- | --- | --- | --- |
| 11-1011.00 | Chief Executives | 10-key calculators | 44101809 | Desktop calculator |
| 11-1011.00 | Chief Executives | Desktop computers | 43211507 | Desktop computers |
| 11-1011.00 | Chief Executives | Laptop computers | 43211503 | Notebook computers |

---

## unspsc_reference

This table contains 4,262 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| commodity_code | BIGINT | |
| commodity_title | VARCHAR | |
| class_code | BIGINT | |
| class_title | VARCHAR | |
| family_code | BIGINT | |
| family_title | VARCHAR | |
| segment_code | BIGINT | |
| segment_title | VARCHAR | |

### Sample Data

| commodity_code | commodity_title | class_code | class_title | family_code | family_title | segment_code | segment_title |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 10111302 | Pet grooming products | 10111300 | Domestic pet treatments and accessories and equ... | 10110000 | Domestic pet products | 10000000 | Live Plant and Animal Material and Accessories ... |
| 10111306 | Domestic pet training kits | 10111300 | Domestic pet treatments and accessories and equ... | 10110000 | Domestic pet products | 10000000 | Live Plant and Animal Material and Accessories ... |
| 10131601 | Cages or its accessories | 10131600 | Animal containment | 10130000 | Animal containment and habitats | 10000000 | Live Plant and Animal Material and Accessories ... |

---

## work_activities

This table contains 72,078 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| onetsoc_code | VARCHAR | |
| title | VARCHAR | |
| element_id | VARCHAR | |
| element_name | VARCHAR | |
| scale_id | VARCHAR | |
| scale_name | VARCHAR | |
| data_value | DOUBLE | |
| n | DOUBLE | |
| standard_error | DOUBLE | |
| lower_ci_bound | DOUBLE | |
| upper_ci_bound | DOUBLE | |
| recommend_suppress | VARCHAR | |
| not_relevant | VARCHAR | |
| date | VARCHAR | |
| domain_source | VARCHAR | |

### Sample Data

| onetsoc_code | title | element_id | element_name | scale_id | scale_name | data_value | n | standard_error | lower_ci_bound | upper_ci_bound | recommend_suppress | not_relevant | date | domain_source |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 11-1011.00 | Chief Executives | 4.A.1.a.1 | Getting Information | IM | Importance | 4.56 | 29.0 | 0.1559 | 4.2369 | 4.8756 | N |  | 08/2023 | Incumbent |
| 11-1011.00 | Chief Executives | 4.A.1.a.1 | Getting Information | LV | Level | 4.89 | 30.0 | 0.1727 | 4.5393 | 5.2458 | N | N | 08/2023 | Incumbent |
| 11-1011.00 | Chief Executives | 4.A.1.a.2 | Monitoring Processes, Materials, or Surroundings | IM | Importance | 4.25 | 30.0 | 0.2125 | 3.813 | 4.6823 | N |  | 08/2023 | Incumbent |

---

## work_context

This table contains 291,201 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| onetsoc_code | VARCHAR | |
| title | VARCHAR | |
| element_id | VARCHAR | |
| element_name | VARCHAR | |
| scale_id | VARCHAR | |
| scale_name | VARCHAR | |
| category | DOUBLE | |
| data_value | DOUBLE | |
| n | DOUBLE | |
| standard_error | DOUBLE | |
| lower_ci_bound | DOUBLE | |
| upper_ci_bound | DOUBLE | |
| recommend_suppress | VARCHAR | |
| not_relevant | DOUBLE | |
| date | VARCHAR | |
| domain_source | VARCHAR | |

### Sample Data

| onetsoc_code | title | element_id | element_name | scale_id | scale_name | category | data_value | n | standard_error | lower_ci_bound | upper_ci_bound | recommend_suppress | not_relevant | date | domain_source |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 11-1011.00 | Chief Executives | 4.C.1.a.2.c | Public Speaking | CX | Context |  | 3.07 | 37.0 | 0.2851 | 2.4923 | 3.6486 | N |  | 08/2023 | Incumbent |
| 11-1011.00 | Chief Executives | 4.C.1.a.2.c | Public Speaking | CXP | Context (Categories 1-5) | 1.0 | 0.13 | 37.0 | 0.137 | 0.016 | 1.077 | N |  | 08/2023 | Incumbent |
| 11-1011.00 | Chief Executives | 4.C.1.a.2.c | Public Speaking | CXP | Context (Categories 1-5) | 2.0 | 39.49 | 37.0 | 11.0101 | 20.4073 | 62.4299 | N |  | 08/2023 | Incumbent |

---

## work_context_categories

This table contains 281 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| element_id | VARCHAR | |
| element_name | VARCHAR | |
| scale_id | VARCHAR | |
| scale_name | VARCHAR | |
| category | BIGINT | |
| category_description | VARCHAR | |

### Sample Data

| element_id | element_name | scale_id | scale_name | category | category_description |
| --- | --- | --- | --- | --- | --- |
| 4.C.1.a.2.c | Public Speaking | CXP | Context (Categories 1-5) | 1 | Never |
| 4.C.1.a.2.c | Public Speaking | CXP | Context (Categories 1-5) | 2 | Once a year or more but not every month |
| 4.C.1.a.2.c | Public Speaking | CXP | Context (Categories 1-5) | 3 | Once a month or more but not every week |

---

## work_styles

This table contains 14,064 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| onetsoc_code | VARCHAR | |
| title | VARCHAR | |
| element_id | VARCHAR | |
| element_name | VARCHAR | |
| scale_id | VARCHAR | |
| scale_name | VARCHAR | |
| data_value | DOUBLE | |
| n | DOUBLE | |
| standard_error | DOUBLE | |
| lower_ci_bound | DOUBLE | |
| upper_ci_bound | DOUBLE | |
| recommend_suppress | VARCHAR | |
| date | VARCHAR | |
| domain_source | VARCHAR | |

### Sample Data

| onetsoc_code | title | element_id | element_name | scale_id | scale_name | data_value | n | standard_error | lower_ci_bound | upper_ci_bound | recommend_suppress | date | domain_source |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 11-1011.00 | Chief Executives | 1.C.1.a | Achievement/Effort | IM | Importance | 4.69 | 28.0 | 0.1218 | 4.4426 | 4.9426 | N | 08/2023 | Incumbent |
| 11-1011.00 | Chief Executives | 1.C.1.b | Persistence | IM | Importance | 4.76 | 28.0 | 0.1104 | 4.5318 | 4.9847 | N | 08/2023 | Incumbent |
| 11-1011.00 | Chief Executives | 1.C.1.c | Initiative | IM | Importance | 4.85 | 28.0 | 0.077 | 4.6927 | 5.0 | N | 08/2023 | Incumbent |

---

## work_values

This table contains 7,866 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| onetsoc_code | VARCHAR | |
| title | VARCHAR | |
| element_id | VARCHAR | |
| element_name | VARCHAR | |
| scale_id | VARCHAR | |
| scale_name | VARCHAR | |
| data_value | DOUBLE | |
| date | VARCHAR | |
| domain_source | VARCHAR | |

### Sample Data

| onetsoc_code | title | element_id | element_name | scale_id | scale_name | data_value | date | domain_source |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 11-1011.00 | Chief Executives | 1.B.2.a | Achievement | EX | Extent | 6.33 | 06/2008 | Analyst |
| 11-1011.00 | Chief Executives | 1.B.2.b | Working Conditions | EX | Extent | 6.33 | 06/2008 | Analyst |
| 11-1011.00 | Chief Executives | 1.B.2.c | Recognition | EX | Extent | 7.0 | 06/2008 | Analyst |

---

