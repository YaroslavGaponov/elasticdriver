const fs = require('fs');
const fuse = require('fuse-bindings');
const elasticsearch = require('elasticsearch');
const debug = require('debug')('elasticdriver');

class LevelInfo {
  constructor(levels) {
    this._levels = levels;
  }
  parse(path) {
    if (path === '/') return {
      level: this._levels[0]
    };
    const result = Object.create(null);
    const arr = path.split('/');
    result.level = this._levels[arr.length - 1];
    for (let i = 0; i < arr.length; i++) {
      result[this._levels[i]] = arr[i];
    }
    return result;
  }
}

const LEVEL = Object.freeze({
  root: 'root',
  link: 'link',
  index: 'index',
  record: 'record'
});

class ElasticAPI {
  constructor(levelInfo) {
    this._parser = levelInfo.parse.bind(levelInfo);
    this._links = new Map();
    this._newFiles = new Set();

    return [
      'create',
      'readdir',
      'getattr',
      'mkdir',
      'rmdir',
      'open',
      'write',
      'read',
      'chmod',
      'unlink'
    ].reduce((o, name) => {
      o[name] = this[name].bind(this);
      return o;
    }, Object.create(null));
  }

  create(path, mode, done) {
    const l = this._parser(path);
    debug(`create ${path} ${JSON.stringify(l)}`);
    this._newFiles.add(path);
    return done(0);
  }

  readdir(path, done) {
    const l = this._parser(path);
    debug(`readdir ${path} ${JSON.stringify(l)}`);

    switch (l.level) {
      case LEVEL.root:
        done(0, [...this._links.keys()]);
        break;
      case LEVEL.link:
        this._links.get(l.link).cat.indices({
            format: 'json',
            h: 'index'
          })
          .then(a => a.map(a => a.index))
          .then(arr => done(0, arr));
        break;
      case LEVEL.index:
        this._links.get(l.link).search({
          index: l.index,
          q: '*'
        }).then(res => done(0, res.hits.hits.map(e => e._id + '.json')));
        break;
      default:
        done(0);
        break;
    }
  }

  getattr(path, done) {
    const l = this._parser(path);
    debug(`getattr ${path} ${JSON.stringify(l)}`);

    switch (l.level) {

      case LEVEL.root:
        done(0, {
          mtime: new Date(),
          atime: new Date(),
          ctime: new Date(),
          nlink: 1,
          size: 100,
          mode: 16877,
          uid: process.getuid ? process.getuid() : 0,
          gid: process.getgid ? process.getgid() : 0
        });
        break;

      case LEVEL.link:
        if (this._links.has(l.link)) {
          done(0, {
            mtime: new Date(),
            atime: new Date(),
            ctime: new Date(),
            nlink: 1,
            size: 100,
            mode: 16877,
            uid: process.getuid ? process.getuid() : 0,
            gid: process.getgid ? process.getgid() : 0
          })
        } else {
          done(fuse.ENOENT);
        }
        break;

      case LEVEL.index:
        this._links.get(l.link).indices.exists({
          index: l.index
        }).then(flag => {
          if (flag) done(0, {
            mtime: new Date(),
            atime: new Date(),
            ctime: new Date(),
            nlink: 1,
            size: 100,
            mode: 16877,
            uid: process.getuid ? process.getuid() : 0,
            gid: process.getgid ? process.getgid() : 0
          });
          else done(fuse.ENOENT);
        }).catch(ex => {
          done(fuse.ENOENT);
        });
        break;

      case LEVEL.record:
        if (this._newFiles.has(path)) {
          done(0, {
            mtime: new Date(),
            atime: new Date(),
            ctime: new Date(),
            nlink: 1,
            size: 1200,
            mode: 33188,
            uid: process.getuid ? process.getuid() : 0,
            gid: process.getgid ? process.getgid() : 0
          })
        } else {
          this._links.get(l.link).exists({
            index: l.index,
            type: 'type',
            id: l.record.split('.')[0]
          }).then(flag => {
            if (flag) {
              done(0, {
                mtime: new Date(),
                atime: new Date(),
                ctime: new Date(),
                nlink: 1,
                size: 1200,
                mode: 33188,
                uid: process.getuid ? process.getuid() : 0,
                gid: process.getgid ? process.getgid() : 0
              })
            } else {
              done(fuse.ENOENT);
            }
          });
        }
        break;

      default:
        done(fuse.ENOENT);
    }
  }

  mkdir(path, mode, done) {
    const l = this._parser(path);
    debug(`mkdir ${path} ${JSON.stringify(l)}`);

    switch (l.level) {
      case LEVEL.link:
        this._links.set(l.link, new elasticsearch.Client({
          host: l.link
        }));
        done(0);
        break;

      case LEVEL.index:
        this._links.get(l.link).indices.create({
          index: l.index
        }).then(_ => done(0)).catch(ex => done(fuse.ENOTDIR));
        break;
      default:
        done(0);
    }
  }

  rmdir(path, done) {
    const l = this._parser(path);
    debug(`rmdir ${path} ${JSON.stringify(l)}`);

    switch (l.level) {
      case LEVEL.link:
        this._links.delete(l.link);
        done(0);
        break;
      case LEVEL.index:
        this._links.get(l.link).indices.delete({
          index: l.index
        }).then(_ => done(0)).catch(ex => done(fuse.ENOTDIR));
        break;
      default:
        done(0);
    }
  }

  open(path, flags, done) {
    const l = this._parser(path);
    debug(`open ${path} ${JSON.stringify(l)}`);

    done(0, 42);
  }

  write(path, fd, buffer, length, position, done) {
    const l = this._parser(path);
    debug(`write ${path} ${JSON.stringify(l)}`);

    this._links.get(l.link).create({
      index: l.index,
      type: 'type',
      id: l.record.split('.')[0],
      body: JSON.parse(buffer.toString('utf8'))
    }).then(_ => done(length)).catch(err => done(0));
  }

  read(path, fd, buffer, length, position, done) {
    const l = this._parser(path);
    debug(`read ${path} ${JSON.stringify(l)}`);

    this._links.get(l.link).get({
      index: l.index,
      type: 'type',
      id: l.record.split('')[0]
    }).then(res => {
      const links = Buffer.from(JSON.stringify(res));
      if (position >= links.length) return done(0);
      var part = links.slice(position, position + length);
      part.copy(buffer);
      done(part.length);
    })
  }

  chmod(path, mode, done) {
    const l = this._parser(path);
    debug(`chmod ${path} ${JSON.stringify(l)}`);

    done(0);
  }

  unlink(path, done) {
    const l = this._parser(path);
    debug(`unlink ${path} ${JSON.stringify(l)}`);

    switch (l.level) {
      case LEVEL.record:
        this._links.get(l.link).delete({
          index: l.index,
          type: 'type',
          id: l.record.split('.')[0]
        }).then(_ => done(0)).catch(err => done(0))
        break;
      default:
        done(0)
    }
  }
}

class ElasticDriver {
  constructor(directory) {
    this._directory = directory;
    this._levelInfo = new LevelInfo([LEVEL.root, LEVEL.link, LEVEL.index, LEVEL.record]);
  }

  mount(done) {
    if (!fs.existsSync(this._directory)) {
      fs.mkdirSync(this._directory);
    }
    const api = new ElasticAPI(this._levelInfo);

    fuse.mount(this._directory, api, done);

    return this;
  }

  unmount(done) {
    fuse.unmount(this._directory, done);
    return this;
  }
}

module.exports = ElasticDriver;