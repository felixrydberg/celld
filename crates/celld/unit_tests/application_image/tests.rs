use super::*;

#[test]
fn images_are_reproducible_hold_nested_files_and_refuse_the_rest() {
    let files = vec![
        ("main.mjs".to_owned(), b"export default {};".to_vec()),
        ("lib/deep/a.mjs".to_owned(), vec![7; 100_000]),
        ("empty".to_owned(), Vec::new()),
    ];
    let image = build(&files).unwrap();
    let mut reordered = files.clone();
    reordered.reverse();
    assert_eq!(image, build(&reordered).unwrap());
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("image");
    std::fs::write(&path, &image).unwrap();
    check(&path).unwrap();
    let extracted = directory.path().join("extracted");
    let status = Command::new("/usr/bin/fsck.erofs")
        .arg(format!("--extract={}", extracted.display()))
        .arg(&path)
        .status()
        .unwrap();
    assert!(status.success());
    for (name, body) in &files {
        assert_eq!(&std::fs::read(extracted.join(name)).unwrap(), body);
    }
    assert!(build(&[("../escape".into(), vec![])]).is_err());
    assert!(build(&[("a".into(), vec![0; crate::applications::MAX_ARTIFACT_BYTES])]).is_err());
    assert!(build(&[("a".into(), vec![]), ("a".into(), vec![])]).is_err());
    let existing = directory.path().join("existing");
    std::fs::write(&existing, b"keep existing image").unwrap();
    assert!(build_file(
        &files,
        &existing,
        crate::applications::MAX_ARTIFACT_BYTES as u64
    )
    .is_err());
    assert_eq!(std::fs::read(&existing).unwrap(), b"keep existing image");
    let dangling = directory.path().join("dangling");
    let absent = directory.path().join("absent");
    std::os::unix::fs::symlink(&absent, &dangling).unwrap();
    assert!(build_file(
        &files,
        &dangling,
        crate::applications::MAX_ARTIFACT_BYTES as u64
    )
    .is_err());
    assert_eq!(std::fs::read_link(&dangling).unwrap(), absent);
    assert!(!absent.exists());
    for refused in [
        Vec::new(),
        vec![0; 4096],
        image[..image.len() - 1].to_vec(),
        image[..image.len() - BLOCK as usize].to_vec(),
    ] {
        std::fs::write(&path, refused).unwrap();
        assert!(check(&path).is_err());
    }
}
