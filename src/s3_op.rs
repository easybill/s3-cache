use std::collections::HashSet;

use hyper::Request;

/// Extracts the S3 operation name (e.g. `"GetObject"`, `"ListBuckets"`) from a raw HTTP request,
/// or `None` if the request does not match any known S3 operation.
pub fn s3_operation_name<B>(req: &Request<B>) -> Option<&'static str> {
    let method = req.method();
    let path = req.uri().path();
    let query = req.uri().query().unwrap_or("");

    // MARK: - Path Shape

    let trimmed = path.trim_start_matches('/');
    let (bucket, raw_key) = match trimmed.find('/') {
        _ if trimmed.is_empty() => (None, None),
        None => (Some(trimmed), None),
        Some(pos) => (Some(&trimmed[..pos]), Some(&trimmed[pos + 1..])),
    };
    let has_key = raw_key.is_some_and(|k| !k.is_empty());

    // MARK: - Query Keys

    let qkeys: HashSet<&str> = query
        .split('&')
        .filter_map(|kv| kv.split('=').next().filter(|k| !k.is_empty()))
        .collect();

    // MARK: - Routing

    use hyper::Method;
    match *method {
        Method::GET => match (bucket.is_some(), has_key) {
            (false, _) => Some("ListBuckets"),
            (true, false) => get_bucket_op(query, &qkeys),
            (true, true) => get_object_op(&qkeys),
        },
        Method::HEAD => match (bucket.is_some(), has_key) {
            (false, _) => None,
            (true, false) => Some("HeadBucket"),
            (true, true) => Some("HeadObject"),
        },
        Method::PUT => match (bucket.is_some(), has_key) {
            (false, _) => None,
            (true, false) => put_bucket_op(&qkeys),
            (true, true) => put_object_op(req, &qkeys),
        },
        Method::DELETE => match (bucket.is_some(), has_key) {
            (false, _) => None,
            (true, false) => delete_bucket_op(&qkeys),
            (true, true) => delete_object_op(&qkeys),
        },
        Method::POST => match (bucket.is_some(), has_key) {
            (false, _) => None,
            (true, false) => post_bucket_op(&qkeys),
            (true, true) => post_object_op(&qkeys),
        },
        _ => None,
    }
}

// MARK: - GET Bucket

fn get_bucket_op(query: &str, qkeys: &HashSet<&str>) -> Option<&'static str> {
    if qkeys.contains("versions") {
        Some("ListObjectVersions")
    } else if qkeys.contains("uploads") {
        Some("ListMultipartUploads")
    } else if query.contains("list-type=2") {
        Some("ListObjectsV2")
    } else if qkeys.contains("location") {
        Some("GetBucketLocation")
    } else if qkeys.contains("acl") {
        Some("GetBucketAcl")
    } else if qkeys.contains("cors") {
        Some("GetBucketCors")
    } else if qkeys.contains("encryption") {
        Some("GetBucketEncryption")
    } else if qkeys.contains("lifecycle") {
        Some("GetBucketLifecycleConfiguration")
    } else if qkeys.contains("logging") {
        Some("GetBucketLogging")
    } else if qkeys.contains("notification") {
        Some("GetBucketNotificationConfiguration")
    } else if qkeys.contains("ownershipControls") {
        Some("GetBucketOwnershipControls")
    } else if qkeys.contains("policyStatus") {
        Some("GetBucketPolicyStatus")
    } else if qkeys.contains("policy") {
        Some("GetBucketPolicy")
    } else if qkeys.contains("replication") {
        Some("GetBucketReplication")
    } else if qkeys.contains("requestPayment") {
        Some("GetBucketRequestPayment")
    } else if qkeys.contains("tagging") {
        Some("GetBucketTagging")
    } else if qkeys.contains("versioning") {
        Some("GetBucketVersioning")
    } else if qkeys.contains("website") {
        Some("GetBucketWebsite")
    } else if qkeys.contains("accelerate") {
        Some("GetBucketAccelerateConfiguration")
    } else if qkeys.contains("publicAccessBlock") {
        Some("GetPublicAccessBlock")
    } else if qkeys.contains("object-lock") {
        Some("GetObjectLockConfiguration")
    } else if qkeys.contains("metadataTable") {
        Some("GetBucketMetadataTableConfiguration")
    } else if qkeys.contains("analytics") {
        if qkeys.contains("id") {
            Some("GetBucketAnalyticsConfiguration")
        } else {
            Some("ListBucketAnalyticsConfigurations")
        }
    } else if qkeys.contains("intelligent-tiering") {
        if qkeys.contains("id") {
            Some("GetBucketIntelligentTieringConfiguration")
        } else {
            Some("ListBucketIntelligentTieringConfigurations")
        }
    } else if qkeys.contains("inventory") {
        if qkeys.contains("id") {
            Some("GetBucketInventoryConfiguration")
        } else {
            Some("ListBucketInventoryConfigurations")
        }
    } else if qkeys.contains("metrics") {
        if qkeys.contains("id") {
            Some("GetBucketMetricsConfiguration")
        } else {
            Some("ListBucketMetricsConfigurations")
        }
    } else {
        Some("ListObjects")
    }
}

// MARK: - GET Object

fn get_object_op(qkeys: &HashSet<&str>) -> Option<&'static str> {
    if qkeys.contains("uploadId") {
        Some("ListParts")
    } else if qkeys.contains("acl") {
        Some("GetObjectAcl")
    } else if qkeys.contains("attributes") {
        Some("GetObjectAttributes")
    } else if qkeys.contains("legal-hold") {
        Some("GetObjectLegalHold")
    } else if qkeys.contains("retention") {
        Some("GetObjectRetention")
    } else if qkeys.contains("tagging") {
        Some("GetObjectTagging")
    } else if qkeys.contains("torrent") {
        Some("GetObjectTorrent")
    } else {
        Some("GetObject")
    }
}

// MARK: - PUT Bucket

fn put_bucket_op(qkeys: &HashSet<&str>) -> Option<&'static str> {
    if qkeys.contains("acl") {
        Some("PutBucketAcl")
    } else if qkeys.contains("cors") {
        Some("PutBucketCors")
    } else if qkeys.contains("encryption") {
        Some("PutBucketEncryption")
    } else if qkeys.contains("lifecycle") {
        Some("PutBucketLifecycleConfiguration")
    } else if qkeys.contains("logging") {
        Some("PutBucketLogging")
    } else if qkeys.contains("notification") {
        Some("PutBucketNotificationConfiguration")
    } else if qkeys.contains("ownershipControls") {
        Some("PutBucketOwnershipControls")
    } else if qkeys.contains("policy") {
        Some("PutBucketPolicy")
    } else if qkeys.contains("replication") {
        Some("PutBucketReplication")
    } else if qkeys.contains("requestPayment") {
        Some("PutBucketRequestPayment")
    } else if qkeys.contains("tagging") {
        Some("PutBucketTagging")
    } else if qkeys.contains("versioning") {
        Some("PutBucketVersioning")
    } else if qkeys.contains("website") {
        Some("PutBucketWebsite")
    } else if qkeys.contains("accelerate") {
        Some("PutBucketAccelerateConfiguration")
    } else if qkeys.contains("publicAccessBlock") {
        Some("PutPublicAccessBlock")
    } else if qkeys.contains("object-lock") {
        Some("PutObjectLockConfiguration")
    } else if qkeys.contains("analytics") {
        Some("PutBucketAnalyticsConfiguration")
    } else if qkeys.contains("intelligent-tiering") {
        Some("PutBucketIntelligentTieringConfiguration")
    } else if qkeys.contains("inventory") {
        Some("PutBucketInventoryConfiguration")
    } else if qkeys.contains("metrics") {
        Some("PutBucketMetricsConfiguration")
    } else {
        Some("CreateBucket")
    }
}

// MARK: - PUT Object

fn put_object_op<B>(req: &Request<B>, qkeys: &HashSet<&str>) -> Option<&'static str> {
    let has_copy_source = req.headers().contains_key("x-amz-copy-source");
    if has_copy_source && qkeys.contains("uploadId") {
        Some("UploadPartCopy")
    } else if has_copy_source {
        Some("CopyObject")
    } else if qkeys.contains("uploadId") {
        Some("UploadPart")
    } else if qkeys.contains("acl") {
        Some("PutObjectAcl")
    } else if qkeys.contains("legal-hold") {
        Some("PutObjectLegalHold")
    } else if qkeys.contains("retention") {
        Some("PutObjectRetention")
    } else if qkeys.contains("tagging") {
        Some("PutObjectTagging")
    } else {
        Some("PutObject")
    }
}

// MARK: - DELETE Bucket

fn delete_bucket_op(qkeys: &HashSet<&str>) -> Option<&'static str> {
    if qkeys.contains("cors") {
        Some("DeleteBucketCors")
    } else if qkeys.contains("encryption") {
        Some("DeleteBucketEncryption")
    } else if qkeys.contains("lifecycle") {
        Some("DeleteBucketLifecycle")
    } else if qkeys.contains("ownershipControls") {
        Some("DeleteBucketOwnershipControls")
    } else if qkeys.contains("policy") {
        Some("DeleteBucketPolicy")
    } else if qkeys.contains("replication") {
        Some("DeleteBucketReplication")
    } else if qkeys.contains("tagging") {
        Some("DeleteBucketTagging")
    } else if qkeys.contains("website") {
        Some("DeleteBucketWebsite")
    } else if qkeys.contains("publicAccessBlock") {
        Some("DeletePublicAccessBlock")
    } else if qkeys.contains("analytics") {
        Some("DeleteBucketAnalyticsConfiguration")
    } else if qkeys.contains("intelligent-tiering") {
        Some("DeleteBucketIntelligentTieringConfiguration")
    } else if qkeys.contains("inventory") {
        Some("DeleteBucketInventoryConfiguration")
    } else if qkeys.contains("metrics") {
        Some("DeleteBucketMetricsConfiguration")
    } else if qkeys.contains("metadataTable") {
        Some("DeleteBucketMetadataTableConfiguration")
    } else {
        Some("DeleteBucket")
    }
}

// MARK: - DELETE Object

fn delete_object_op(qkeys: &HashSet<&str>) -> Option<&'static str> {
    if qkeys.contains("uploadId") {
        Some("AbortMultipartUpload")
    } else if qkeys.contains("tagging") {
        Some("DeleteObjectTagging")
    } else {
        Some("DeleteObject")
    }
}

// MARK: - POST Bucket

fn post_bucket_op(qkeys: &HashSet<&str>) -> Option<&'static str> {
    if qkeys.contains("delete") {
        Some("DeleteObjects")
    } else {
        // `POST /{bucket}` with no recognized query key is the URL shape of
        // `PostObject` (HTML form-based upload, RFC 2388). However, `PostObject`
        // carries no discriminating query parameter — it is only distinguishable
        // from an unrecognized bucket-level POST by inspecting the request body or
        // `Content-Type: multipart/form-data` header, which is outside the scope
        // of URI-only routing. Returning `None` avoids a false positive.
        None
    }
}

// MARK: - POST Object

fn post_object_op(qkeys: &HashSet<&str>) -> Option<&'static str> {
    if qkeys.contains("uploads") {
        Some("CreateMultipartUpload")
    } else if qkeys.contains("uploadId") {
        Some("CompleteMultipartUpload")
    } else if qkeys.contains("restore") {
        Some("RestoreObject")
    } else if qkeys.contains("select") {
        Some("SelectObjectContent")
    } else {
        // No S3 operation uses `POST /{bucket}/{key}` without a query key.
        None
    }
}

// MARK: - Tests

#[cfg(test)]
mod tests;
