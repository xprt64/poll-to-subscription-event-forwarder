/*
 * Copyright (c) 2017 Constantin Galbenu <xprt64@gmail.com>
 */

const http = require('http');
const eachLimit = require('async/eachLimit');
const request = require('request');
const fs = require('fs');
const fileName = '/data/lastProcessedCommit.json';
let lastProcessedCommit = {};
const indexUrl = process.argv[2] || 'http://10.5.5.137/eventstore';
const notifyUrl = process.argv[3] || 'http://10.5.5.137/eventstore/new/event';
let eventsUrlTemplate;

if (!fs.existsSync('/data')) {
    fs.mkdirSync('/data', 0o777);
}

lastProcessedCommit.load = function () {
    "use strict";
    
    let data = {
        sequence: -1,
        index: -1,
    };
    
    try {
        data = JSON.parse(fs.readFileSync(fileName, 'utf8'))
    }
    catch (e) {
        
    }
    
    this.sequence = data.sequence;
    this.index = data.index;
};

lastProcessedCommit.save = function () {
    "use strict";
    
    fs.writeFileSync(fileName, JSON.stringify(this), {encoding: 'utf8'});
};

lastProcessedCommit.incrementSequence = function (sequence) {
    "use strict";
    this.sequence = sequence;
    this.index = -1;
    this.save();
    console.log(`incremented sequence ${this.sequence}`);
};

lastProcessedCommit.incrementEvent = function () {
    "use strict";
    this.index++;
    this.save();
    console.log(`incremented index ${this.index}`);
};

lastProcessedCommit.load();

discoverLink.retry = 0;
main();

function main() {
    "use strict";
    
    discoverLink().then(pollCommits).catch((err) => {
        console.error(`REST ERROR: Failed to discover event store links, retrying...`);
        console.error(err);
        discoverLink.retry++;
        setTimeout(main, 3000);
    })
}

function pollCommits() {
    "use strict";
    
    const urlString = eventsUrlTemplate.replace('{after}', lastProcessedCommit.sequence).replace('{limit}', 10);
    
    console.log(`pollCommits ${urlString}`);
    
    return getJson(urlString).then((body) => {
        return processCommits(body['stream']);
    }).then(pollCommits).catch(reschedulePollingCommits);
}

function reschedulePollingCommits() {
    "use strict";
    
    setTimeout(pollCommits, 3000);
}

function processCommits(commits) {
    "use strict";
    
    console.log(`retrived ${commits.length} commits`);
    
    return new Promise((resolve, reject) => {
        eachLimit(commits, 1, processCommit, (err) => err ? reject() : resolve());
    });
}

function processCommit(commit, callback) {
    "use strict";
    
    console.log(`Process commit #${commit.sequence}`);
    
    if (commit.sequence <= lastProcessedCommit.sequence) {
        console.log(`already processed #${commit.sequence}`);
        callback();
        return;
    }
    
    let unprocessedEvents = commit.events
        .filter((event, index) => index > lastProcessedCommit.index)
        .map((event, index) => {
            event.sequence = commit.sequence;
            event.index = index;
            return event;
        });
    
    eachLimit(unprocessedEvents, 1, processEvent, (err) => {
        if (err) {
            callback(err);
        }
        else {
            lastProcessedCommit.incrementSequence(commit.sequence);
            callback();
        }
    });
}

function processEvent(event, callback) {
    "use strict";
    
    console.log(`Process event`);
    
    sendEvent(event).then(function (rsp) {
        lastProcessedCommit.incrementEvent();
        callback();
    }).catch(function (error) {
        console.error("Event processing failed, retrying...", error);
        setTimeout(() => processEvent(event, callback), 3000);
    });
}

function sendEvent(event) {
    "use strict";
    
    console.log(`sendEvent event at ${notifyUrl}`, event);
    
    return postJson(notifyUrl, event);
}

function discoverLink() {
    "use strict";
    
    console.log(`discoverLink ${indexUrl}`);
    if (discoverLink.retry > 0) {
        console.log(`retry #${discoverLink.retry}`);
    }
    
    return getJson(indexUrl).then((body) => {
        eventsUrlTemplate = body.links.filter(link => "after_sequence" == link.rel)[0].href;
        console.log(`event store link discovered: ${eventsUrlTemplate} `);
    });
}

function postJson(url, data) {
    "use strict";
    
    return new Promise((resolve, reject) => {
        request({
            url: url,
            method: 'POST',
            json: data,
            
        }, function (error, response, body) {
            
            if (response && response.statusCode == 200 && typeof body === 'object') {
                resolve(body);
            }
            else {
                if (!response) {
                    reject({code: error.errno, message: JSON.stringify(error), body: null});
                }
                else {
                    reject({code: response.statusCode, message: response.statusMessage, body: body});
                }
            }
        });
    });
}


function getJson(url) {
    "use strict";
    
    return new Promise((resolve, reject) => {
        request({
            url: url,
            method: 'GET',
            json: true,
            
        }, function (error, response, body) {
            if (response && response.statusCode == 200 && typeof body === 'object') {
                resolve(body);
            }
            else {
                if (!response) {
                    reject({code: error.errno, message: JSON.stringify(error), body: null});
                }
                else {
                    reject({code: response.statusCode, message: response.statusText, body: body});
                }
            }
        });
    });
}