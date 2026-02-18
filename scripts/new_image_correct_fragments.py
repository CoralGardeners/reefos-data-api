# -*- coding: utf-8 -*-

import reefos_data_api.query_firestore as qq

creds = 'restoration-ios-firebase-adminsdk-wg0a4-18ff398018.json'
project_id="restoration-ios"

gcs_image_path = 'images/'

qf = qq.QueryFirestore(project_id=project_id, creds=creds)
uf = qq.UpdateFirestore(qf)

bucket = qf.get_bucket("restoration-ios.appspot.com")

# %%
blobs = bucket.list_blobs(prefix=gcs_image_path)

# get the images of each fragment from the GCS blob names
image_data = {}
for idx, blob in enumerate(blobs):
    if idx % 100 == 0:
        print(f"Processing {idx}")
    name_parts = blob.name.split('/')
    doc_id = name_parts[1]
    img_name = name_parts[-1]
    size = img_name.split('.')[0].split('-')[1]
    view = img_name.split('.')[0].split('-')[0]
    if not doc_id in image_data:
        image_data[doc_id] = {}
    imgs = image_data[doc_id]
    if view not in imgs:
        imgs[view] = {'side': view}
    imgs[view][size] = blob.name

# %%
event_coll = "_events"
site_coll = "_sites"

idx = 0
for doc_id, images in image_data.items():
    if idx % 100 == 0:
        print(f"Processing {idx} of {len(image_data)}")
    idx += 1
#    if idx < 800:
#        continue
    ev = qf.get_document(event_coll, doc_id)
    if ev is not None:
        if type(ev) is dict:
            if 'eventData' not in ev:
                ev['eventData'] = {}
            ev['eventData']['images'] = list(images.values())
            uf.update_document(event_coll, doc_id, ev)
        else:
            print("ev not as expected {ev}")
#    else:
#        site = qf.get_document(site_coll, doc_id)
#        if site is not None:
#            site['siteData']['images'] = list(images.values())
#            #uf.update_document(site_coll, doc_id, ev)
        