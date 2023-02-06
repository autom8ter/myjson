
function setDocTimestamp(doc) {
    doc.Set('timestamp', new Date().toISOString())
}

function isSuperUser(meta) {
    return contains(meta.Get('roles'), 'super_user')
}

function accountQueryAuth(query, meta) {
    return query.where?.length > 0 && query.where[0].field == '_id' && query.where[0].op == 'eq' && contains(meta.Get('groups'), query.where[0].value)
}