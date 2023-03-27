'use strict';

import { parseQuickQuery, getSchemaData } from './library/helper';

let onRun = function(context) {
    let queryEditor = context.currentQueryEditor();
    if (queryEditor == null) {
        context.alert('Error', 'No SQL Editor');
        return;
    }
    const range = queryEditor.currentSelectedRange();
    const quickQuery = queryEditor.currentSelectedString();

    getSchemaData(context, schemaData => {
        let sqlQuery = parseQuickQuery(context, quickQuery, schemaData);
        queryEditor.replaceStringInRange(sqlQuery, range);
    })
};

let printSchema = function (context) {
    let queryEditor = context.currentQueryEditor();
    if (queryEditor == null) {
        context.alert('Error', 'No SQL Editor');
        return;
    }
    const range = queryEditor.currentSelectedRange();
    getSchemaData(context, schemaData => {
        queryEditor.replaceStringInRange(JSON.stringify(schemaData), range);
    })
}

global.onRun = onRun;
global.printSchema = printSchema;