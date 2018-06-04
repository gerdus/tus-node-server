'use strict';

const DataStore = require('./DataStore');
const File = require('../models/File');
const fs = require('fs');
const crypto = require('crypto');
const writeFileAtomic = require('write-file-atomic');
const MASK = '0777';
const IGNORED_MKDIR_ERROR = 'EEXIST';
const FILE_DOESNT_EXIST = 'ENOENT';
const ERRORS = require('../constants').ERRORS;
const EVENTS = require('../constants').EVENTS;
const debug = require('debug');
const log = debug('tus-node-server:stores:filestore');

/**
 * @fileOverview
 * Store using local filesystem.
 *
 * @author Ben Stahl <bhstahl@gmail.com>
 */

class FileStore extends DataStore {
    constructor(options) {
        super(options);

        this.directory = options.directory || options.path.replace(/^\//, '');

        this.extensions = ['creation', 'creation-defer-length', 'checksum'];
        this.extensionHeaders = { 'Tus-Checksum-Algorithm': 'sha1' };
        this._checkOrCreateDirectory();
    }

    /**
     *  Ensure the directory exists.
     */
    _checkOrCreateDirectory() {
        fs.mkdir(this.directory, MASK, (error) => {
            if (error && error.code !== IGNORED_MKDIR_ERROR) {
                throw error;
            }
        });
    }

    /**
     * Create an empty file.
     *
     * @param  {object} req http.incomingMessage
     * @param  {File} file
     * @return {Promise}
     */
    create(req) {
        return new Promise((resolve, reject) => {
            const upload_length = req.headers['upload-length'];
            const upload_defer_length = req.headers['upload-defer-length'];
            const upload_metadata = req.headers['upload-metadata'];

            if (upload_length === undefined && upload_defer_length === undefined) {
                return reject(ERRORS.INVALID_LENGTH);
            }

            let file_id;
            try {
                file_id = this.generateFileName(req);
            }
            catch (generateError) {
                log('[FileStore] create: check your namingFunction. Error', generateError);
                return reject(ERRORS.FILE_WRITE_ERROR);
            }

            const file = new File(file_id, upload_length, upload_defer_length, upload_metadata);
            const filepath = this.filePath(file_id);

            return fs.open(filepath, 'w', (err, fd) => {
                if (err) {
                    log('[FileStore] create: Error', err);
                    return reject(err);
                }

                try {
                    this.writeFileInfo(file);
                }
                catch (infoWriteError) {
                    log('[FileStore] create: Info Write Error', infoWriteError);
                    return reject(infoWriteError);
                }


                return fs.close(fd, (exception) => {
                    if (exception) {
                        log('[FileStore] create: Error', exception);
                        return reject(exception);
                    }

                    this.emit(EVENTS.EVENT_FILE_CREATED, { file });
                    return resolve(file);
                });
            });
        });
    }

    calculateSHA1File(fn) {
        return new Promise((resolve, reject) => {
            const algo = 'sha1';
            const shasum = crypto.createHash(algo);

            const s = fs.ReadStream(fn);
            s.on('data', (d) => {
                shasum.update(d);
            });
            s.on('end', () => {
                const d = shasum.digest('base64');
                s.close();
                resolve(d);
            });
        });

    }

    /**
     * Write to the file, starting at the provided offset
     *
     * @param  {object} req http.incomingMessage
     * @param  {string} file_id   Name of file
     * @param  {integer} offset     starting offset
     * @return {Promise}
     */
    write(req, file_id, offset) {
        return new Promise((resolve, reject) => {
            const path = this.filePath(file_id);
            const tempchunkpath = `${path}.tmp`;

            const tempstream = fs.createWriteStream(tempchunkpath, { flags: 'w' });
            tempstream.on('error', (e) => {
                log('[FileStore] write: Error', e);
                reject(ERRORS.FILE_WRITE_ERROR);
            });

            let new_offset = 0;
            req.on('data', (buffer) => {
                new_offset += buffer.length;
            });

            return req.pipe(tempstream).on('finish', async () => {

                const upload_checksum = req.headers['upload-checksum'];
                if (upload_checksum !== undefined) {
                    const upload_checksum_algorithm = upload_checksum.split(' ')[0];
                    const upload_checksum_value = upload_checksum.split(' ')[1];
                    if (upload_checksum_algorithm !== 'sha1') {
                        return reject(ERRORS.CHECKSUM_ALGORITHM_UNSUPPORTED);
                    }
                    const sha1hash = await this.calculateSHA1File(tempchunkpath);
                    if (upload_checksum_value !== sha1hash) {
                        return reject(ERRORS.CHECKSUM_MISMATCH);
                    }
                }

                const tempreadstream = fs.createReadStream(tempchunkpath, { flags: 'r' });
                const options = {
                    flags: 'r+',
                    start: offset,
                };

                const stream = fs.createWriteStream(path, options);

                stream.on('error', (e) => {
                    log('[FileStore] write: Error', e);
                    reject(ERRORS.FILE_WRITE_ERROR);
                });

                return tempreadstream.pipe(stream).on('finish', () => {
                    fs.unlink(tempchunkpath, (delchunkpatherr) => {
                        // whatever the result OK
                    });
                    log(`[FileStore] write: ${new_offset} bytes written to ${path}`);
                    offset += new_offset;
                    log(`[FileStore] write: File is now ${offset} bytes`);

                    const config = this.getFileInfo(file_id);
                    if (config && parseInt(config.upload_length, 10) === offset) {
                        this.emit(EVENTS.EVENT_UPLOAD_COMPLETE, { file: config });
                    }
                    resolve(offset);
                });

            });


        });
    }

    /**
     * Return file stats, if they exits
     *
     * @param  {string} file_id name of the file
     * @return {object}           fs stats
     */
    getOffset(file_id) {
        const config = this.getFileInfo(file_id);
        return new Promise((resolve, reject) => {
            const file_path = this.filePath(file_id);
            fs.stat(file_path, (error, stats) => {
                if (error && error.code === FILE_DOESNT_EXIST && config) {
                    log(`[FileStore] getOffset: No file found at ${file_path} but db record exists`, config);
                    return reject(ERRORS.FILE_NO_LONGER_EXISTS);
                }

                if (error && error.code === FILE_DOESNT_EXIST) {
                    log(`[FileStore] getOffset: No file found at ${file_path}`);
                    return reject(ERRORS.FILE_NOT_FOUND);
                }

                if (error) {
                    return reject(error);
                }

                if (stats.isDirectory()) {
                    log(`[FileStore] getOffset: ${file_path} is a directory`);
                    return reject(ERRORS.FILE_NOT_FOUND);
                }

                const data = Object.assign(stats, config);
                return resolve(data);
            });
        });
    }

    /**
     * Return file path
     *
     * @param  {string} file_id of the file
     * @return {string} file path
     */
    filePath(file_id) {
        return `${this.directory}/${file_id}`;
    }

    /**
     * infoPath returns the path to the .info file storing the file's info.
     *
     * @param  {string} file_id of the file
     * @return {string} .info file path
     */
    infoPath(file_id) {
        return `${this.directory}/${file_id}.info`;
    }

    /**
     * Return file info
     *
     * @param  {string} file_id of the file
     * @return {object} File object or null on failure or does not exist
     */
    getFileInfo(file_id) {
        try {
            const infoFilename = this.infoPath(file_id);
            const data = fs.readFileSync(infoFilename);
            const file = JSON.parse(data);
            return file;
        }
        catch (e) {
            log(`[FileStore] getInfo: failure reading info for file id ${file_id} `, e);
            return null;
        }
    }

    /**
     * writes the file info to .info file. Overwrites.
     *
     * @param {object} file - File object
     */
    writeFileInfo(file) {
        const infoFilename = this.infoPath(file.id);
        const data = JSON.stringify(file);
        writeFileAtomic.sync(infoFilename, data);
    }
}

module.exports = FileStore;
