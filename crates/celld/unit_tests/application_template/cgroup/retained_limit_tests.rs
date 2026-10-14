use super::*;

#[test]
fn ceiling_alignment_is_checked_and_never_rounds_down() {
    let page = page_aligned(1).unwrap();
    assert!(page > 0);
    assert_eq!(page_aligned(page).unwrap(), page);
    assert_eq!(page_aligned(page + 1).unwrap(), page * 2);
    assert!(page_aligned(u64::MAX).is_err());
}
