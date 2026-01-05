import reefos_data_api.query_firestore as qq
from reefos_data_api.firestore_constants import EventType as et

# %%
org_name = 'coral-gardeners'

fp_branch_name = 'French Polynesia'
fiji_branch_name = 'Fiji'
thailand_branch_name = 'Thailand'

creds = 'restoration-ios-firebase-adminsdk-wg0a4-18ff398018.json'
project_id="restoration-ios"

qf = qq.QueryFirestore(project_id=project_id, creds=creds)
uf = qq.UpdateFirestore(qf)

cg_org_id = qf.get_org_by_name("Coral Gardeners")[0]
cg_org_path = 'coral-gardeners'

cc_org_id = qf.get_org_by_name("Coral Catch")[0]
cc_org_path = 'coral-catch'

org_paths = {cg_org_id: cg_org_path, cc_org_id: cc_org_path}

fp_branch_id = qf.get_branch_by_name(cg_org_id, fp_branch_name)[0]
fiji_branch_id = qf.get_branch_by_name(cg_org_id, fiji_branch_name)[0]
thailand_branch_id = qf.get_branch_by_name(cg_org_id, thailand_branch_name)[0]
giliair_branch_id = qf.get_branch_by_name(cc_org_id, "Gili Air")[0]

branch_paths = {
    fp_branch_id: "moorea",
    fiji_branch_id: "fiji",
    thailand_branch_id: "thailand",
    giliair_branch_id: "gili-air",
    }

# %%
# mother colonies

cg_mc_sites = qf.get_docs(qf.query_donorcolonies({'orgID': cg_org_id}))
cc_mc_sites = qf.get_docs(qf.query_donorcolonies({'orgID': cc_org_id}))

# nursery monitoring events

cg_monitorings = qf.get_docs(qf.query_events({'orgID': cg_org_id},
                                             event_type=et.frag_monitor.value))
cc_monitorings = qf.get_docs(qf.query_events({'orgID': cc_org_id},
                                             event_type=et.frag_monitor.value))

# %%
debug = False

bucket = qf.get_bucket("restoration-ios.appspot.com")

# %%
mc_sites = cg_mc_sites + cc_mc_sites
cnt = 1
for site_id, mc_site in mc_sites:
    print(f"Updating {cnt} of {len(mc_sites)}")
    loc = mc_site['location']
    images = mc_site['siteData']['images']
    new_images = []
    for image in images:
        new_image = {}
        for img_type, img_name in image.items():
            name_parts = img_name.split('/')
            if len(name_parts) > 2 and name_parts[1] == 'images':
                full_name = (org_paths[loc['orgID']] + '/' + 
                             branch_paths[loc['branchID']] +
                             img_name)
                new_name = name_parts[-1]
                new_path = 'images/' + site_id + '/' + new_name
                # copy
                print(f"Copy {full_name} to {new_path}")
                if not debug:
                    blob = bucket.blob(full_name)
                    if not blob.exists():
                        continue
                    blob_copy = bucket.copy_blob(blob,
                                                 bucket,
                                                 new_path)            
                new_image[img_type] = new_path
        if len(new_image) > 0:
            new_images.append(new_image)
    # update site data
    print(new_images)
    if len(new_images) > 0 and not debug:
        mc_site['siteData']['images'] = new_images
        uf.update_document('_sites', site_id, mc_site)
    cnt += 1

mon_events = cg_monitorings + cc_monitorings
cnt = 1
for event_id, event in mon_events:
    print(f"Updating {cnt} of {len(mon_events)}")
    loc = event['location']
    images = event['eventData']['images']
    new_images = []
    for image in images:
        new_image = {}
        for img_type, img_name in image.items():
            if img_type == 'side':
                new_image[img_type] = img_name
                continue
            name_parts = img_name.split('/')
            if len(name_parts) > 2 and name_parts[1] == 'images':
                full_name = (org_paths[loc['orgID']] + '/' + 
                             branch_paths[loc['branchID']] +
                             img_name)
                new_name = name_parts[-1]
                new_path = 'images/' + site_id + '/' + new_name
                # copy
                print(f"Copy {full_name} to {new_path}")
                if not debug:
                    blob = bucket.blob(full_name)
                    if not blob.exists():
                        continue
                    blob_copy = bucket.copy_blob(blob,
                                                 bucket,
                                                 new_path)            
                new_image[img_type] = new_path
        if len(new_image) > 1:
            new_images.append(new_image)
    # update site data
    print(new_images)
    if len(new_images) > 0 and not debug:
        mc_site['siteData']['images'] = new_images
        uf.update_document('_events', event_id, event)
    cnt += 1
