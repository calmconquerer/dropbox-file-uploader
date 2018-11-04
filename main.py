import dropbox
import config
import os


token = config.exports['token']
dbx = dropbox.Dropbox(token)


sourceFolder = config.exports['sourceFolder']
targetFolder = config.exports['targetFolder']


chunk_size = config.exports['chunkSize'] * 1024 * 1024


# walk return first the current folder that it walk, then tuples of dirs and files not "subdir, dirs, files"
for dirpath, dirnames, files in os.walk(sourceFolder):
    for file in files:
        try:
            file_path = os.path.join(dirpath, file)
            destination_path = os.path.join(targetFolder, file)
            print('Uploading %s to %s' % (file_path, destination_path))
            with open(file_path, 'rb') as f:
                # if file == '.DS_Store':
                #     pass
                fileSize = os.path.getsize(file_path)
                if fileSize <= chunk_size:
                    if file == '.DS_Store':
                        pass
                    else:
                        dbx.files_upload(f.read(), destination_path)
                else:
                    upload_session_start_result = dbx.files_upload_session_start(f.read(chunk_size))
                    cursor = dropbox.files.UploadSessionCursor(session_id=upload_session_start_result.session_id,
                                                               offset=f.tell())
                    commit = dropbox.files.CommitInfo(path=destination_path)

                    while f.tell() < fileSize:
                        if (fileSize - f.tell()) <= chunk_size:
                            print(dbx.files_upload_session_finish(f.read(chunk_size), cursor, commit))
                        else:
                            dbx.files_upload_session_append_v2(f.read(chunk_size), cursor)
                            cursor.offset = f.tell()
            os.unlink(file_path)
            f.close()
        except Exception as err:
                print("Failed to upload %s\n%s" % (file, err))
print("Finished upload.")

