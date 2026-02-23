import reefos_data_api.query_firestore as qq

creds = 'restoration-ios-firebase-adminsdk-wg0a4-18ff398018.json'
project_id="restoration-ios"

qf = qq.QueryFirestore(project_id=project_id, creds=creds)

# get org and branch
org_name = 'Coral Gardeners'
org_id = qf.get_org_by_name(org_name)[0]
loc = {'orgID': org_id}
branch = qf.get_branch_by_name(org_id, "French Polynesia")

# %%
keep = ['organismID', 'createdAt']

# get all donor colonies for French Polynesia branch
loc = {'branchID': branch[0]}
dc = qf.get_docs(qf.query_donorcolonies(loc))
df = qf.documents_to_dataframe(dc, to_explode=['siteData'])[keep]
# %%
# filter by year
_df = df[df.createdAt.dt.year == 2025]
# get taxon names
_df['taxon'] = _df.organismID.map(qf.get_coral_taxon_map())
# print the results
print(_df.taxon.value_counts())
