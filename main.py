import dropbox
import config
import os
from fluent import sender
from fluent import event

# Token taken from the configuration file
dbx = dropbox.Dropbox(config.exports['token'])

# Setup fluentd for logging
sender.setup('fluentd.test', host='localhost', port=24224)

chunk_size = config.exports['chunkSize'] * 1024 * 1024
max_chunk_size = 2 * 1024 * 1024

'''
    - Basically it traverses over the source folder and then takes each file in the source folder and uploads it to 
      dropbox directly if it's less than the max chunk size (max chunk size is 150 in the dropbox api but for my 
      convenience I made it 2(poor internet connection)).
    
    - If the file is larger than the max chunk size, it creates an upload session for the file to get uploaded in small 
      chunks.
    
    - Exception handling in this is somewhat poor but I'll definitely improve it.
    
    - Added the fluentd logger in the Exception handling block to log the errors since the app will run in the 
      background
      
'''
for dirpath, dirnames, files in os.walk(config.exports['sourceFolder']):
    for file in files:
        try:
            file_path = os.path.join(dirpath, file)
            destination_path = os.path.join(config.exports['targetFolder'], file)
            print('Uploading {} to {}'.format(file_path, destination_path))
            with open(file_path, 'rb') as f:
                # if file == '.DS_Store':
                #     pass
                fileSize = os.path.getsize(file_path)
                if fileSize <= max_chunk_size:
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
            print("Failed to upload {}\n{}".format(file, err))
            event.Event('follow', {
                'Failed to upload ': file,
                'Error': err
            })

print("Finished upload.")

