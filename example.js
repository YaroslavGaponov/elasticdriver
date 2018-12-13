const ElasticDriver = require('./index');

const driver = new ElasticDriver(__dirname + '/elasticsearch');

driver.mount(err => console.log('mount ', err ? err : 'ok'));

process.on('SIGINT', code => {
    driver.unmount(err => console.log('unmount ', err ? err : 'ok'));
});