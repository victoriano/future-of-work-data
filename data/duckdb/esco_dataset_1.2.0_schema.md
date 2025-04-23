# esco_dataset_1.2.0 Database Schema

This document describes the schema of the esco_dataset_1.2.0 database.

## Tables

- [ISCOGroups_en](#iscogroups_en)
- [broaderRelationsOccPillar_en](#broaderrelationsoccpillar_en)
- [broaderRelationsSkillPillar_en](#broaderrelationsskillpillar_en)
- [conceptSchemes_en](#conceptschemes_en)
- [digCompSkillsCollection_en](#digcompskillscollection_en)
- [digitalSkillsCollection_en](#digitalskillscollection_en)
- [greenSkillsCollection_en](#greenskillscollection_en)
- [languageSkillsCollection_en](#languageskillscollection_en)
- [occupationSkillRelations_en](#occupationskillrelations_en)
- [occupation_hierarchy](#occupation_hierarchy)
- [occupation_skills](#occupation_skills)
- [occupations_en](#occupations_en)
- [researchOccupationsCollection_en](#researchoccupationscollection_en)
- [researchSkillsCollection_en](#researchskillscollection_en)
- [skillGroups_en](#skillgroups_en)
- [skillSkillRelations_en](#skillskillrelations_en)
- [skillsHierarchy_en](#skillshierarchy_en)
- [skills_en](#skills_en)
- [transversalSkillsCollection_en](#transversalskillscollection_en)

## ISCOGroups_en

This table contains 619 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| conceptType | VARCHAR | |
| conceptUri | VARCHAR | |
| code | VARCHAR | |
| preferredLabel | VARCHAR | |
| status | VARCHAR | |
| altLabels | VARCHAR | |
| inScheme | VARCHAR | |
| description | VARCHAR | |

### Sample Data

| conceptType | conceptUri | code | preferredLabel | status | altLabels | inScheme | description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ISCOGroup | http://data.europa.eu/esco/isco/C0 | 0 | Armed forces occupations | released |  | http://data.europa.eu/esco/concept-scheme/occup... | Armed forces occupations include all jobs held ... |
| ISCOGroup | http://data.europa.eu/esco/isco/C01 | 01 | Commissioned armed forces officers | released |  | http://data.europa.eu/esco/concept-scheme/isco,... | Commissioned armed forces officers provide lead... |
| ISCOGroup | http://data.europa.eu/esco/isco/C011 | 011 | Commissioned armed forces officers | released |  | http://data.europa.eu/esco/concept-scheme/isco,... | Commissioned armed forces officers provide lead... |

---

## broaderRelationsOccPillar_en

This table contains 3,652 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| conceptType | VARCHAR | |
| conceptUri | VARCHAR | |
| broaderType | VARCHAR | |
| broaderUri | VARCHAR | |

### Sample Data

| conceptType | conceptUri | broaderType | broaderUri |
| --- | --- | --- | --- |
| ISCOGroup | http://data.europa.eu/esco/isco/C01 | ISCOGroup | http://data.europa.eu/esco/isco/C0 |
| ISCOGroup | http://data.europa.eu/esco/isco/C011 | ISCOGroup | http://data.europa.eu/esco/isco/C01 |
| ISCOGroup | http://data.europa.eu/esco/isco/C0110 | ISCOGroup | http://data.europa.eu/esco/isco/C011 |

---

## broaderRelationsSkillPillar_en

This table contains 20,822 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| conceptType | VARCHAR | |
| conceptUri | VARCHAR | |
| broaderType | VARCHAR | |
| broaderUri | VARCHAR | |

### Sample Data

| conceptType | conceptUri | broaderType | broaderUri |
| --- | --- | --- | --- |
| SkillGroup | http://data.europa.eu/esco/isced-f/00 | SkillGroup | http://data.europa.eu/esco/skill/c46fcb45-5c14-... |
| SkillGroup | http://data.europa.eu/esco/isced-f/000 | SkillGroup | http://data.europa.eu/esco/isced-f/00 |
| SkillGroup | http://data.europa.eu/esco/isced-f/0000 | SkillGroup | http://data.europa.eu/esco/isced-f/000 |

---

## conceptSchemes_en

This table contains 19 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| conceptType | VARCHAR | |
| conceptSchemeUri | VARCHAR | |
| preferredLabel | VARCHAR | |
| title | VARCHAR | |
| status | VARCHAR | |
| description | VARCHAR | |
| hasTopConcept | VARCHAR | |

### Sample Data

| conceptType | conceptSchemeUri | preferredLabel | title | status | description | hasTopConcept |
| --- | --- | --- | --- | --- | --- | --- |
| ConceptScheme | http://data.europa.eu/esco/concept-scheme/6c930... | Digital |  | released |  | http://data.europa.eu/esco/skill/dad7e408-162f-... |
| ConceptScheme | http://data.europa.eu/esco/concept-scheme/8ae4b... | Research occupations |  | released |  | http://data.europa.eu/esco/occupation/a9c7a04d-... |
| ConceptScheme | http://data.europa.eu/esco/concept-scheme/digcomp | DigComp |  | released |  | http://data.europa.eu/esco/skill/7e5147d1-60b1-... |

---

## digCompSkillsCollection_en

This table contains 25 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| conceptType | VARCHAR | |
| conceptUri | VARCHAR | |
| preferredLabel | VARCHAR | |
| status | VARCHAR | |
| skillType | VARCHAR | |
| reuseLevel | VARCHAR | |
| altLabels | VARCHAR | |
| description | VARCHAR | |
| broaderConceptUri | VARCHAR | |
| broaderConceptPT | VARCHAR | |

### Sample Data

| conceptType | conceptUri | preferredLabel | status | skillType | reuseLevel | altLabels | description | broaderConceptUri | broaderConceptPT |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| KnowledgeSkillCompetence | http://data.europa.eu/esco/skill/14832d87-2f2f-... | solve technical problems | released | skill/competence | cross-sector |  | Identify technical problems when operating devi... | http://data.europa.eu/esco/skill/a628d2d1-f40a-... | identify problems \| resolving computer problem... |
| KnowledgeSkillCompetence | http://data.europa.eu/esco/skill/16a00c69-9c74-... | use e-services | released | skill/competence | cross-sector | apply e-services \| able to use e-services \| u... | Participate in society through the use of publi... | http://data.europa.eu/esco/skill/98fb499f-9155-... | using digital tools for collaboration and produ... |
| KnowledgeSkillCompetence | http://data.europa.eu/esco/skill/1a4cc54f-1e53-... | creatively use digital technologies | released | skill/competence | cross-sector |  | Use digital tools and technologies to create kn... | http://data.europa.eu/esco/skill/98fb499f-9155-... | using digital tools for collaboration and produ... |

---

## digitalSkillsCollection_en

This table contains 1,284 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| conceptType | VARCHAR | |
| conceptUri | VARCHAR | |
| preferredLabel | VARCHAR | |
| status | VARCHAR | |
| skillType | VARCHAR | |
| reuseLevel | VARCHAR | |
| altLabels | VARCHAR | |
| description | VARCHAR | |
| broaderConceptUri | VARCHAR | |
| broaderConceptPT | VARCHAR | |

### Sample Data

| conceptType | conceptUri | preferredLabel | status | skillType | reuseLevel | altLabels | description | broaderConceptUri | broaderConceptPT |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| KnowledgeSkillCompetence | http://data.europa.eu/esco/skill/000f1d3d-220f-... | Haskell | released | knowledge | sector-specific | Haskell techniques | The techniques and principles of software devel... | http://data.europa.eu/esco/skill/21d2f96d-35f7-... | computer programming \| software and applicatio... |
| KnowledgeSkillCompetence | http://data.europa.eu/esco/skill/00c04e40-35ea-... | incremental development | released | knowledge | sector-specific | gradual development | The incremental development model is a methodol... | http://data.europa.eu/esco/isced-f/0613 \| http... | software and applications development and analy... |
| KnowledgeSkillCompetence | http://data.europa.eu/esco/skill/013441c1-1f13-... | KDevelop | released | knowledge | sector-specific | kdevplatform \| KDevelop platform | The computer program KDevelop is a suite of sof... | http://data.europa.eu/esco/skill/925463a7-d51f-... | integrated development environment software \| ... |

---

## greenSkillsCollection_en

This table contains 591 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| conceptType | VARCHAR | |
| conceptUri | VARCHAR | |
| preferredLabel | VARCHAR | |
| status | VARCHAR | |
| skillType | VARCHAR | |
| reuseLevel | VARCHAR | |
| altLabels | VARCHAR | |
| description | VARCHAR | |
| broaderConceptUri | VARCHAR | |
| broaderConceptPT | VARCHAR | |

### Sample Data

| conceptType | conceptUri | preferredLabel | status | skillType | reuseLevel | altLabels | description | broaderConceptUri | broaderConceptPT |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| KnowledgeSkillCompetence | http://data.europa.eu/esco/skill/001d46db-035e-... | train staff to reduce food waste | released | skill/competence | sector-specific | teach students food waste reduction practices \... | Establish new trainings and staff development p... | http://data.europa.eu/esco/skill/6c4fa8c8-e9e1-... | training on operational procedures |
| KnowledgeSkillCompetence | http://data.europa.eu/esco/skill/0037c821-2898-... | develop energy saving concepts | released | skill/competence | cross-sector | create concepts for energy saving \| energy sav... | Use current research results and collaborate wi... | http://data.europa.eu/esco/skill/c23e0a2f-f04b-... | developing operational policies and procedures ... |
| KnowledgeSkillCompetence | http://data.europa.eu/esco/skill/0058526a-11e9-... | conduct research on flora | released | skill/competence | sector-specific | carry out research on flora \| flora research \... | Collect and analyse data about plants in order ... | http://data.europa.eu/esco/skill/ba1f6201-b206-... | analysing scientific and medical data |

---

## languageSkillsCollection_en

This table contains 359 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| conceptType | VARCHAR | |
| conceptUri | VARCHAR | |
| skillType | VARCHAR | |
| reuseLevel | VARCHAR | |
| preferredLabel | VARCHAR | |
| status | VARCHAR | |
| altLabels | VARCHAR | |
| description | VARCHAR | |
| broaderConceptUri | VARCHAR | |
| broaderConceptPT | VARCHAR | |

### Sample Data

| conceptType | conceptUri | skillType | reuseLevel | preferredLabel | status | altLabels | description | broaderConceptUri | broaderConceptPT |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| KnowledgeSkillCompetence | http://data.europa.eu/esco/skill/01f10952-cb59-... | skill/competence | transversal | write Hungarian | released | correspond in written Hungarian \| show compete... | Compose written texts in Hungarian. | http://data.europa.eu/esco/skill/ddd3596a-43f3-... | Hungarian |
| KnowledgeSkillCompetence | http://data.europa.eu/esco/skill/021c8a09-6b8d-... | knowledge | transversal | understand spoken Luxembourgish | released | comprehend spoken Luxembourgish \| interpret sp... | Comprehend orally expressed Luxembourgish. | http://data.europa.eu/esco/skill/7d16f1e4-1003-... | Luxembourgish |
| KnowledgeSkillCompetence | http://data.europa.eu/esco/skill/02d68c2b-1722-... | skill/competence | transversal | understand written Korean | released | understanding written Korean \| read Korean \| ... | Read and comprehend written texts in Korean. | http://data.europa.eu/esco/skill/f9bc2890-d1f2-... | Korean |

---

## occupationSkillRelations_en

This table contains 129,004 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| occupationUri | VARCHAR | |
| relationType | VARCHAR | |
| skillType | VARCHAR | |
| skillUri | VARCHAR | |

### Sample Data

| occupationUri | relationType | skillType | skillUri |
| --- | --- | --- | --- |
| http://data.europa.eu/esco/occupation/00030d09-... | essential | knowledge | http://data.europa.eu/esco/skill/fed5b267-73fa-... |
| http://data.europa.eu/esco/occupation/00030d09-... | essential | skill/competence | http://data.europa.eu/esco/skill/05bc7677-5a64-... |
| http://data.europa.eu/esco/occupation/00030d09-... | essential | skill/competence | http://data.europa.eu/esco/skill/271a36a0-bc7a-... |

---

## occupation_hierarchy

This table contains 3,652 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| conceptUri | VARCHAR | |
| broaderUri | VARCHAR | |
| occupation_name | VARCHAR | |
| broader_name | VARCHAR | |

### Sample Data

| conceptUri | broaderUri | occupation_name | broader_name |
| --- | --- | --- | --- |
| http://data.europa.eu/esco/occupation/000e93a3-... | http://data.europa.eu/esco/isco/C8121 | metal drawing machine operator |  |
| http://data.europa.eu/esco/occupation/0022f466-... | http://data.europa.eu/esco/isco/C3155 | air traffic safety technician |  |
| http://data.europa.eu/esco/occupation/002da35b-... | http://data.europa.eu/esco/isco/C2431 | hospitality revenue manager |  |

---

## occupation_skills

This table contains 129,004 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| occupation_name | VARCHAR | |
| occupation_uri | VARCHAR | |
| skill_name | VARCHAR | |
| skill_uri | VARCHAR | |
| relationType | VARCHAR | |

### Sample Data

| occupation_name | occupation_uri | skill_name | skill_uri | relationType |
| --- | --- | --- | --- | --- |
| technical director | http://data.europa.eu/esco/occupation/00030d09-... | theatre techniques | http://data.europa.eu/esco/skill/fed5b267-73fa-... | essential |
| technical director | http://data.europa.eu/esco/occupation/00030d09-... | organise rehearsals | http://data.europa.eu/esco/skill/05bc7677-5a64-... | essential |
| technical director | http://data.europa.eu/esco/occupation/00030d09-... | write risk assessment on performing arts produc... | http://data.europa.eu/esco/skill/271a36a0-bc7a-... | essential |

---

## occupations_en

This table contains 3,039 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| conceptType | VARCHAR | |
| conceptUri | VARCHAR | |
| iscoGroup | VARCHAR | |
| preferredLabel | VARCHAR | |
| altLabels | VARCHAR | |
| hiddenLabels | VARCHAR | |
| status | VARCHAR | |
| modifiedDate | TIMESTAMP | |
| regulatedProfessionNote | VARCHAR | |
| scopeNote | VARCHAR | |
| definition | VARCHAR | |
| inScheme | VARCHAR | |
| description | VARCHAR | |
| code | VARCHAR | |

### Sample Data

| conceptType | conceptUri | iscoGroup | preferredLabel | altLabels | hiddenLabels | status | modifiedDate | regulatedProfessionNote | scopeNote | definition | inScheme | description | code |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Occupation | http://data.europa.eu/esco/occupation/00030d09-... | 2654 | technical director | technical and operations director
head of techn... |  | released | 2024-01-25 11:28:50.295000 | http://data.europa.eu/esco/regulated-profession... |  |  | http://data.europa.eu/esco/concept-scheme/occup... | Technical directors realise the artistic vision... | 2654.1.7 |
| Occupation | http://data.europa.eu/esco/occupation/000e93a3-... | 8121 | metal drawing machine operator | metal drawing machine technician
metal drawing ... |  | released | 2024-01-23 10:09:32.099000 | http://data.europa.eu/esco/regulated-profession... |  |  | http://data.europa.eu/esco/concept-scheme/membe... | Metal drawing machine operators set up and oper... | 8121.4 |
| Occupation | http://data.europa.eu/esco/occupation/0019b951-... | 7543 | precision device inspector | inspector of precision instruments
precision de... |  | released | 2024-01-25 15:00:12.188000 | http://data.europa.eu/esco/regulated-profession... |  |  | http://data.europa.eu/esco/concept-scheme/occup... | Precision device inspectors make sure precision... | 7543.10.3 |

---

## researchOccupationsCollection_en

This table contains 122 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| conceptType | VARCHAR | |
| conceptUri | VARCHAR | |
| preferredLabel | VARCHAR | |
| status | VARCHAR | |
| altLabels | VARCHAR | |
| description | VARCHAR | |
| broaderConceptUri | VARCHAR | |
| broaderConceptPT | VARCHAR | |

### Sample Data

| conceptType | conceptUri | preferredLabel | status | altLabels | description | broaderConceptUri | broaderConceptPT |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Occupation | http://data.europa.eu/esco/occupation/01ffb917-... | biomedical engineer | released | BME consultant \| bio-medical engineer \| biome... | Biomedical engineers combine knowledge of engin... | http://data.europa.eu/esco/occupation/f9433fdb-... | bioengineer |
| Occupation | http://data.europa.eu/esco/occupation/0611f232-... | criminologist | released | criminology science researcher \| criminology s... | Criminologists study conditions pertaining to h... | http://data.europa.eu/esco/isco/C2632 | Sociologists, anthropologists and related profe... |
| Occupation | http://data.europa.eu/esco/occupation/0959cd1d-... | economics lecturer | released | economics teacher \| university economics lectu... | Economics lecturers are subject professors, ass... | http://data.europa.eu/esco/occupation/684fd8d5-... | higher education lecturer |

---

## researchSkillsCollection_en

This table contains 40 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| conceptType | VARCHAR | |
| conceptUri | VARCHAR | |
| preferredLabel | VARCHAR | |
| status | VARCHAR | |
| skillType | VARCHAR | |
| reuseLevel | VARCHAR | |
| altLabels | VARCHAR | |
| description | VARCHAR | |
| broaderConceptUri | VARCHAR | |
| broaderConceptPT | VARCHAR | |

### Sample Data

| conceptType | conceptUri | preferredLabel | status | skillType | reuseLevel | altLabels | description | broaderConceptUri | broaderConceptPT |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| KnowledgeSkillCompetence | http://data.europa.eu/esco/skill/00b9a3aa-7070-... | draft scientific or academic papers and technic... | released | skill/competence | cross-sector | write scientific and academic papers \| write t... | Draft and edit scientific, academic or technica... | http://data.europa.eu/esco/skill/6e62e776-fbfa-... | technical or academic writing |
| KnowledgeSkillCompetence | http://data.europa.eu/esco/skill/08b04e53-ed25-... | manage research data | released | skill/competence | cross-sector | administer research data \| handle research dat... | Produce and analyse scientific data originating... | http://data.europa.eu/esco/skill/32c017fd-28ab-... | managing information |
| KnowledgeSkillCompetence | http://data.europa.eu/esco/skill/20a8fe89-d4eb-... | interact professionally in research and profess... | released | skill/competence | cross-sector | interact appropriately in research and professi... | Show consideration to others as well as collegi... | http://data.europa.eu/esco/skill/91b0b918-942e-... | working with others |

---

## skillGroups_en

This table contains 640 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| conceptType | VARCHAR | |
| conceptUri | VARCHAR | |
| preferredLabel | VARCHAR | |
| altLabels | VARCHAR | |
| hiddenLabels | VARCHAR | |
| status | VARCHAR | |
| modifiedDate | TIMESTAMP | |
| scopeNote | VARCHAR | |
| inScheme | VARCHAR | |
| description | VARCHAR | |
| code | VARCHAR | |

### Sample Data

| conceptType | conceptUri | preferredLabel | altLabels | hiddenLabels | status | modifiedDate | scopeNote | inScheme | description | code |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| SkillGroup | http://data.europa.eu/esco/isced-f/00 | generic programmes and qualifications |  |  | released |  |  | http://data.europa.eu/esco/concept-scheme/isced... | Generic programmes and qualifications are those... | 00 |
| SkillGroup | http://data.europa.eu/esco/isced-f/000 | generic programmes and qualifications not furth... |  |  | released | 2023-07-13 15:16:44.968000 |  | http://data.europa.eu/esco/concept-scheme/skill... |  | 000 |
| SkillGroup | http://data.europa.eu/esco/isced-f/0000 | generic programmes and qualifications not furth... |  |  | released |  |  | http://data.europa.eu/esco/concept-scheme/skill... |  | 0000 |

---

## skillSkillRelations_en

This table contains 5,818 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| originalSkillUri | VARCHAR | |
| originalSkillType | VARCHAR | |
| relationType | VARCHAR | |
| relatedSkillType | VARCHAR | |
| relatedSkillUri | VARCHAR | |

### Sample Data

| originalSkillUri | originalSkillType | relationType | relatedSkillType | relatedSkillUri |
| --- | --- | --- | --- | --- |
| http://data.europa.eu/esco/skill/00064735-8fad-... | knowledge | optional | knowledge | http://data.europa.eu/esco/skill/d4a0744a-508b-... |
| http://data.europa.eu/esco/skill/000bb1e4-89f0-... | knowledge | optional | knowledge | http://data.europa.eu/esco/skill/b70ab677-5781-... |
| http://data.europa.eu/esco/skill/0023e7a5-43da-... | knowledge | optional | knowledge | http://data.europa.eu/esco/skill/5753e2ca-8934-... |

---

## skillsHierarchy_en

This table contains 640 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| Level 0 URI | VARCHAR | |
| Level 0 preferred term | VARCHAR | |
| Level 1 URI | VARCHAR | |
| Level 1 preferred term | VARCHAR | |
| Level 2 URI | VARCHAR | |
| Level 2 preferred term | VARCHAR | |
| Level 3 URI | VARCHAR | |
| Level 3 preferred term | VARCHAR | |
| Description | VARCHAR | |
| Scope note | VARCHAR | |
| Level 0 code | VARCHAR | |
| Level 1 code | VARCHAR | |
| Level 2 code | VARCHAR | |
| Level 3 code | VARCHAR | |

### Sample Data

| Level 0 URI | Level 0 preferred term | Level 1 URI | Level 1 preferred term | Level 2 URI | Level 2 preferred term | Level 3 URI | Level 3 preferred term | Description | Scope note | Level 0 code | Level 1 code | Level 2 code | Level 3 code |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| http://data.europa.eu/esco/skill/e35a5936-091d-... | language skills and knowledge |  |  |  |  |  |  |  |  | L |  |  |  |
| http://data.europa.eu/esco/skill/e35a5936-091d-... | language skills and knowledge | http://data.europa.eu/esco/skill/43f425aa-f45d-... | languages |  |  |  |  | Ability to communicate through reading, writing... |  | L | L1 |  |  |
| http://data.europa.eu/esco/skill/e35a5936-091d-... | language skills and knowledge | http://data.europa.eu/esco/skill/e434e71a-f068-... | classical languages |  |  |  |  | All dead languages, no longer actively used, or... | Excludes:
-  all languages that are actively us... | L | L2 |  |  |

---

## skills_en

This table contains 13,939 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| conceptType | VARCHAR | |
| conceptUri | VARCHAR | |
| skillType | VARCHAR | |
| reuseLevel | VARCHAR | |
| preferredLabel | VARCHAR | |
| altLabels | VARCHAR | |
| hiddenLabels | VARCHAR | |
| status | VARCHAR | |
| modifiedDate | TIMESTAMP | |
| scopeNote | VARCHAR | |
| definition | VARCHAR | |
| inScheme | VARCHAR | |
| description | VARCHAR | |

### Sample Data

| conceptType | conceptUri | skillType | reuseLevel | preferredLabel | altLabels | hiddenLabels | status | modifiedDate | scopeNote | definition | inScheme | description |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| KnowledgeSkillCompetence | http://data.europa.eu/esco/skill/0005c151-5b5a-... | skill/competence | sector-specific | manage musical staff | manage staff of music
coordinate duties of musi... |  | released | 2023-11-30 15:53:37.136000 |  |  | http://data.europa.eu/esco/concept-scheme/skill... | Assign and manage staff tasks in areas such as ... |
| KnowledgeSkillCompetence | http://data.europa.eu/esco/skill/00064735-8fad-... | skill/competence | occupation-specific | supervise correctional procedures | oversee prison procedures
manage correctional p... |  | released | 2023-11-30 15:04:00.689000 |  |  | http://data.europa.eu/esco/concept-scheme/membe... | Supervise the operations of a correctional faci... |
| KnowledgeSkillCompetence | http://data.europa.eu/esco/skill/000709ed-2be5-... | skill/competence | sector-specific | apply anti-oppressive practices | apply non-oppressive practices
apply an anti-op... |  | released | 2023-11-28 10:45:53.540000 |  |  | http://data.europa.eu/esco/concept-scheme/skill... | Identify oppression in societies, economies, cu... |

---

## transversalSkillsCollection_en

This table contains 95 records.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| conceptType | VARCHAR | |
| conceptUri | VARCHAR | |
| skillType | VARCHAR | |
| reuseLevel | VARCHAR | |
| preferredLabel | VARCHAR | |
| status | VARCHAR | |
| altLabels | VARCHAR | |
| description | VARCHAR | |
| broaderConceptUri | VARCHAR | |
| broaderConceptPT | VARCHAR | |

### Sample Data

| conceptType | conceptUri | skillType | reuseLevel | preferredLabel | status | altLabels | description | broaderConceptUri | broaderConceptPT |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| KnowledgeSkillCompetence | http://data.europa.eu/esco/skill/001115fb-569f-... | skill/competence | transversal | show initiative | released | take the initiative \| give impetus \| be a dri... | Be proactive and take the first step in an acti... | http://data.europa.eu/esco/skill/91860993-1a8b-... | taking a proactive approach |
| KnowledgeSkillCompetence | http://data.europa.eu/esco/skill/0171653e-c8e9-... | skill/competence | transversal | adopt ways to foster biodiversity and animal we... | released | implement environmental choices in your own eat... | Engage in behaviours that help maintaining stab... | http://data.europa.eu/esco/skill/80cf002a-6586-... | applying environmental skills and competences |
| KnowledgeSkillCompetence | http://data.europa.eu/esco/skill/045f71e6-0699-... | skill/competence | transversal | advise others | released | make recommendations to others \| provide advis... | Offer suggestions about the best course of action. | http://data.europa.eu/esco/skill/82463bb1-85d1-... | supporting others |

---

