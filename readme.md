ElasticDriver
=================

Working with ElasticSearch via Filesystem API

# Demo
![demo](https://raw.githubusercontent.com/YaroslavGaponov/elasticdriver/master/images/demo.gif "demo")

# Example

```javascript
const ElasticDriver = require('./index');

const driver = new ElasticDriver(__dirname + '/elasticsearch');

driver.mount(err => console.log('mount ', err ? err : 'ok'));

process.on('SIGINT', code => {
    driver.unmount(err => console.log('unmount ', err ? err : 'ok'));
});
```