use super::*;
use hyper::Request;

fn req(method: &str, uri: &str) -> Request<()> {
    Request::builder().method(method).uri(uri).body(()).unwrap()
}

fn req_with_header(method: &str, uri: &str, key: &str, val: &str) -> Request<()> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header(key, val)
        .body(())
        .unwrap()
}

// MARK: - GET Root

#[test]
fn list_buckets() {
    assert_eq!(s3_operation_name(&req("GET", "/")), Some("ListBuckets"));
}

// MARK: - GET Bucket

#[test]
fn list_object_versions() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket?versions")),
        Some("ListObjectVersions")
    );
}

#[test]
fn list_multipart_uploads() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket?uploads")),
        Some("ListMultipartUploads")
    );
}

#[test]
fn list_objects_v2() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket?list-type=2")),
        Some("ListObjectsV2")
    );
}

#[test]
fn list_objects() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket")),
        Some("ListObjects")
    );
}

#[test]
fn get_bucket_location() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket?location")),
        Some("GetBucketLocation")
    );
}

#[test]
fn get_bucket_acl() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket?acl")),
        Some("GetBucketAcl")
    );
}

#[test]
fn get_bucket_cors() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket?cors")),
        Some("GetBucketCors")
    );
}

#[test]
fn get_bucket_encryption() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket?encryption")),
        Some("GetBucketEncryption")
    );
}

#[test]
fn get_bucket_lifecycle_configuration() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket?lifecycle")),
        Some("GetBucketLifecycleConfiguration")
    );
}

#[test]
fn get_bucket_logging() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket?logging")),
        Some("GetBucketLogging")
    );
}

#[test]
fn get_bucket_notification_configuration() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket?notification")),
        Some("GetBucketNotificationConfiguration")
    );
}

#[test]
fn get_bucket_ownership_controls() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket?ownershipControls")),
        Some("GetBucketOwnershipControls")
    );
}

#[test]
fn get_bucket_policy() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket?policy")),
        Some("GetBucketPolicy")
    );
}

#[test]
fn get_bucket_policy_status() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket?policyStatus")),
        Some("GetBucketPolicyStatus")
    );
}

#[test]
fn get_bucket_replication() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket?replication")),
        Some("GetBucketReplication")
    );
}

#[test]
fn get_bucket_request_payment() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket?requestPayment")),
        Some("GetBucketRequestPayment")
    );
}

#[test]
fn get_bucket_tagging() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket?tagging")),
        Some("GetBucketTagging")
    );
}

#[test]
fn get_bucket_versioning() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket?versioning")),
        Some("GetBucketVersioning")
    );
}

#[test]
fn get_bucket_website() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket?website")),
        Some("GetBucketWebsite")
    );
}

#[test]
fn get_bucket_accelerate_configuration() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket?accelerate")),
        Some("GetBucketAccelerateConfiguration")
    );
}

#[test]
fn get_public_access_block() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket?publicAccessBlock")),
        Some("GetPublicAccessBlock")
    );
}

#[test]
fn get_object_lock_configuration() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket?object-lock")),
        Some("GetObjectLockConfiguration")
    );
}

#[test]
fn get_bucket_metadata_table_configuration() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket?metadataTable")),
        Some("GetBucketMetadataTableConfiguration")
    );
}

#[test]
fn get_bucket_analytics_configuration() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket?analytics&id=my-id")),
        Some("GetBucketAnalyticsConfiguration")
    );
}

#[test]
fn list_bucket_analytics_configurations() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket?analytics")),
        Some("ListBucketAnalyticsConfigurations")
    );
}

#[test]
fn get_bucket_intelligent_tiering_configuration() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket?intelligent-tiering&id=my-id")),
        Some("GetBucketIntelligentTieringConfiguration")
    );
}

#[test]
fn list_bucket_intelligent_tiering_configurations() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket?intelligent-tiering")),
        Some("ListBucketIntelligentTieringConfigurations")
    );
}

#[test]
fn get_bucket_inventory_configuration() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket?inventory&id=my-id")),
        Some("GetBucketInventoryConfiguration")
    );
}

#[test]
fn list_bucket_inventory_configurations() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket?inventory")),
        Some("ListBucketInventoryConfigurations")
    );
}

#[test]
fn get_bucket_metrics_configuration() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket?metrics&id=my-id")),
        Some("GetBucketMetricsConfiguration")
    );
}

#[test]
fn list_bucket_metrics_configurations() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket?metrics")),
        Some("ListBucketMetricsConfigurations")
    );
}

// MARK: - GET Object

#[test]
fn list_parts() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket/key?uploadId=abc")),
        Some("ListParts")
    );
}

#[test]
fn get_object_acl() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket/key?acl")),
        Some("GetObjectAcl")
    );
}

#[test]
fn get_object_attributes() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket/key?attributes")),
        Some("GetObjectAttributes")
    );
}

#[test]
fn get_object_legal_hold() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket/key?legal-hold")),
        Some("GetObjectLegalHold")
    );
}

#[test]
fn get_object_retention() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket/key?retention")),
        Some("GetObjectRetention")
    );
}

#[test]
fn get_object_tagging() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket/key?tagging")),
        Some("GetObjectTagging")
    );
}

#[test]
fn get_object_torrent() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket/key?torrent")),
        Some("GetObjectTorrent")
    );
}

#[test]
fn get_object() {
    assert_eq!(
        s3_operation_name(&req("GET", "/bucket/key")),
        Some("GetObject")
    );
}

// MARK: - HEAD

#[test]
fn head_bucket() {
    assert_eq!(
        s3_operation_name(&req("HEAD", "/bucket")),
        Some("HeadBucket")
    );
}

#[test]
fn head_object() {
    assert_eq!(
        s3_operation_name(&req("HEAD", "/bucket/key")),
        Some("HeadObject")
    );
}

// MARK: - PUT Bucket

#[test]
fn create_bucket() {
    assert_eq!(
        s3_operation_name(&req("PUT", "/bucket")),
        Some("CreateBucket")
    );
}

#[test]
fn put_bucket_acl() {
    assert_eq!(
        s3_operation_name(&req("PUT", "/bucket?acl")),
        Some("PutBucketAcl")
    );
}

#[test]
fn put_bucket_cors() {
    assert_eq!(
        s3_operation_name(&req("PUT", "/bucket?cors")),
        Some("PutBucketCors")
    );
}

#[test]
fn put_bucket_encryption() {
    assert_eq!(
        s3_operation_name(&req("PUT", "/bucket?encryption")),
        Some("PutBucketEncryption")
    );
}

#[test]
fn put_bucket_lifecycle_configuration() {
    assert_eq!(
        s3_operation_name(&req("PUT", "/bucket?lifecycle")),
        Some("PutBucketLifecycleConfiguration")
    );
}

#[test]
fn put_bucket_logging() {
    assert_eq!(
        s3_operation_name(&req("PUT", "/bucket?logging")),
        Some("PutBucketLogging")
    );
}

#[test]
fn put_bucket_notification_configuration() {
    assert_eq!(
        s3_operation_name(&req("PUT", "/bucket?notification")),
        Some("PutBucketNotificationConfiguration")
    );
}

#[test]
fn put_bucket_ownership_controls() {
    assert_eq!(
        s3_operation_name(&req("PUT", "/bucket?ownershipControls")),
        Some("PutBucketOwnershipControls")
    );
}

#[test]
fn put_bucket_policy() {
    assert_eq!(
        s3_operation_name(&req("PUT", "/bucket?policy")),
        Some("PutBucketPolicy")
    );
}

#[test]
fn put_bucket_replication() {
    assert_eq!(
        s3_operation_name(&req("PUT", "/bucket?replication")),
        Some("PutBucketReplication")
    );
}

#[test]
fn put_bucket_request_payment() {
    assert_eq!(
        s3_operation_name(&req("PUT", "/bucket?requestPayment")),
        Some("PutBucketRequestPayment")
    );
}

#[test]
fn put_bucket_tagging() {
    assert_eq!(
        s3_operation_name(&req("PUT", "/bucket?tagging")),
        Some("PutBucketTagging")
    );
}

#[test]
fn put_bucket_versioning() {
    assert_eq!(
        s3_operation_name(&req("PUT", "/bucket?versioning")),
        Some("PutBucketVersioning")
    );
}

#[test]
fn put_bucket_website() {
    assert_eq!(
        s3_operation_name(&req("PUT", "/bucket?website")),
        Some("PutBucketWebsite")
    );
}

#[test]
fn put_bucket_accelerate_configuration() {
    assert_eq!(
        s3_operation_name(&req("PUT", "/bucket?accelerate")),
        Some("PutBucketAccelerateConfiguration")
    );
}

#[test]
fn put_public_access_block() {
    assert_eq!(
        s3_operation_name(&req("PUT", "/bucket?publicAccessBlock")),
        Some("PutPublicAccessBlock")
    );
}

#[test]
fn put_object_lock_configuration() {
    assert_eq!(
        s3_operation_name(&req("PUT", "/bucket?object-lock")),
        Some("PutObjectLockConfiguration")
    );
}

#[test]
fn put_bucket_analytics_configuration() {
    assert_eq!(
        s3_operation_name(&req("PUT", "/bucket?analytics")),
        Some("PutBucketAnalyticsConfiguration")
    );
}

#[test]
fn put_bucket_intelligent_tiering_configuration() {
    assert_eq!(
        s3_operation_name(&req("PUT", "/bucket?intelligent-tiering")),
        Some("PutBucketIntelligentTieringConfiguration")
    );
}

#[test]
fn put_bucket_inventory_configuration() {
    assert_eq!(
        s3_operation_name(&req("PUT", "/bucket?inventory")),
        Some("PutBucketInventoryConfiguration")
    );
}

#[test]
fn put_bucket_metrics_configuration() {
    assert_eq!(
        s3_operation_name(&req("PUT", "/bucket?metrics")),
        Some("PutBucketMetricsConfiguration")
    );
}

// MARK: - PUT Object

#[test]
fn put_object() {
    assert_eq!(
        s3_operation_name(&req("PUT", "/bucket/key")),
        Some("PutObject")
    );
}

#[test]
fn put_object_acl() {
    assert_eq!(
        s3_operation_name(&req("PUT", "/bucket/key?acl")),
        Some("PutObjectAcl")
    );
}

#[test]
fn put_object_legal_hold() {
    assert_eq!(
        s3_operation_name(&req("PUT", "/bucket/key?legal-hold")),
        Some("PutObjectLegalHold")
    );
}

#[test]
fn put_object_retention() {
    assert_eq!(
        s3_operation_name(&req("PUT", "/bucket/key?retention")),
        Some("PutObjectRetention")
    );
}

#[test]
fn put_object_tagging() {
    assert_eq!(
        s3_operation_name(&req("PUT", "/bucket/key?tagging")),
        Some("PutObjectTagging")
    );
}

#[test]
fn copy_object() {
    assert_eq!(
        s3_operation_name(&req_with_header(
            "PUT",
            "/bucket/key",
            "x-amz-copy-source",
            "/src-bucket/src-key"
        )),
        Some("CopyObject")
    );
}

#[test]
fn upload_part() {
    assert_eq!(
        s3_operation_name(&req("PUT", "/bucket/key?uploadId=abc&partNumber=1")),
        Some("UploadPart")
    );
}

#[test]
fn upload_part_copy() {
    assert_eq!(
        s3_operation_name(&req_with_header(
            "PUT",
            "/bucket/key?uploadId=abc&partNumber=1",
            "x-amz-copy-source",
            "/src-bucket/src-key"
        )),
        Some("UploadPartCopy")
    );
}

// MARK: - DELETE Bucket

#[test]
fn delete_bucket() {
    assert_eq!(
        s3_operation_name(&req("DELETE", "/bucket")),
        Some("DeleteBucket")
    );
}

#[test]
fn delete_bucket_cors() {
    assert_eq!(
        s3_operation_name(&req("DELETE", "/bucket?cors")),
        Some("DeleteBucketCors")
    );
}

#[test]
fn delete_bucket_encryption() {
    assert_eq!(
        s3_operation_name(&req("DELETE", "/bucket?encryption")),
        Some("DeleteBucketEncryption")
    );
}

#[test]
fn delete_bucket_lifecycle() {
    assert_eq!(
        s3_operation_name(&req("DELETE", "/bucket?lifecycle")),
        Some("DeleteBucketLifecycle")
    );
}

#[test]
fn delete_bucket_ownership_controls() {
    assert_eq!(
        s3_operation_name(&req("DELETE", "/bucket?ownershipControls")),
        Some("DeleteBucketOwnershipControls")
    );
}

#[test]
fn delete_bucket_policy() {
    assert_eq!(
        s3_operation_name(&req("DELETE", "/bucket?policy")),
        Some("DeleteBucketPolicy")
    );
}

#[test]
fn delete_bucket_replication() {
    assert_eq!(
        s3_operation_name(&req("DELETE", "/bucket?replication")),
        Some("DeleteBucketReplication")
    );
}

#[test]
fn delete_bucket_tagging() {
    assert_eq!(
        s3_operation_name(&req("DELETE", "/bucket?tagging")),
        Some("DeleteBucketTagging")
    );
}

#[test]
fn delete_bucket_website() {
    assert_eq!(
        s3_operation_name(&req("DELETE", "/bucket?website")),
        Some("DeleteBucketWebsite")
    );
}

#[test]
fn delete_public_access_block() {
    assert_eq!(
        s3_operation_name(&req("DELETE", "/bucket?publicAccessBlock")),
        Some("DeletePublicAccessBlock")
    );
}

#[test]
fn delete_bucket_analytics_configuration() {
    assert_eq!(
        s3_operation_name(&req("DELETE", "/bucket?analytics")),
        Some("DeleteBucketAnalyticsConfiguration")
    );
}

#[test]
fn delete_bucket_intelligent_tiering_configuration() {
    assert_eq!(
        s3_operation_name(&req("DELETE", "/bucket?intelligent-tiering")),
        Some("DeleteBucketIntelligentTieringConfiguration")
    );
}

#[test]
fn delete_bucket_inventory_configuration() {
    assert_eq!(
        s3_operation_name(&req("DELETE", "/bucket?inventory")),
        Some("DeleteBucketInventoryConfiguration")
    );
}

#[test]
fn delete_bucket_metrics_configuration() {
    assert_eq!(
        s3_operation_name(&req("DELETE", "/bucket?metrics")),
        Some("DeleteBucketMetricsConfiguration")
    );
}

#[test]
fn delete_bucket_metadata_table_configuration() {
    assert_eq!(
        s3_operation_name(&req("DELETE", "/bucket?metadataTable")),
        Some("DeleteBucketMetadataTableConfiguration")
    );
}

// MARK: - DELETE Object

#[test]
fn delete_object() {
    assert_eq!(
        s3_operation_name(&req("DELETE", "/bucket/key")),
        Some("DeleteObject")
    );
}

#[test]
fn delete_object_tagging() {
    assert_eq!(
        s3_operation_name(&req("DELETE", "/bucket/key?tagging")),
        Some("DeleteObjectTagging")
    );
}

#[test]
fn abort_multipart_upload() {
    assert_eq!(
        s3_operation_name(&req("DELETE", "/bucket/key?uploadId=abc")),
        Some("AbortMultipartUpload")
    );
}

// MARK: - POST Bucket

#[test]
fn delete_objects() {
    assert_eq!(
        s3_operation_name(&req("POST", "/bucket?delete")),
        Some("DeleteObjects")
    );
}

// MARK: - POST Object

#[test]
fn create_multipart_upload() {
    assert_eq!(
        s3_operation_name(&req("POST", "/bucket/key?uploads")),
        Some("CreateMultipartUpload")
    );
}

#[test]
fn complete_multipart_upload() {
    assert_eq!(
        s3_operation_name(&req("POST", "/bucket/key?uploadId=abc")),
        Some("CompleteMultipartUpload")
    );
}

#[test]
fn restore_object() {
    assert_eq!(
        s3_operation_name(&req("POST", "/bucket/key?restore")),
        Some("RestoreObject")
    );
}

#[test]
fn select_object_content() {
    assert_eq!(
        s3_operation_name(&req("POST", "/bucket/key?select&select-type=2")),
        Some("SelectObjectContent")
    );
}

// MARK: - None Cases

#[test]
fn none_unknown_method() {
    assert_eq!(s3_operation_name(&req("PATCH", "/bucket/key")), None);
}

#[test]
fn none_head_root() {
    assert_eq!(s3_operation_name(&req("HEAD", "/")), None);
}

#[test]
fn none_put_root() {
    assert_eq!(s3_operation_name(&req("PUT", "/")), None);
}

#[test]
fn none_delete_root() {
    assert_eq!(s3_operation_name(&req("DELETE", "/")), None);
}

#[test]
fn none_post_root() {
    assert_eq!(s3_operation_name(&req("POST", "/")), None);
}

#[test]
fn none_post_bucket_no_known_key() {
    assert_eq!(s3_operation_name(&req("POST", "/bucket")), None);
}

#[test]
fn none_post_object_no_known_key() {
    assert_eq!(s3_operation_name(&req("POST", "/bucket/key")), None);
}
