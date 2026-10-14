use super::*;

#[cfg(test)]
pub(crate) async fn invoke<T: Send + 'static, F: Future<Output = Result<T>> + Send + 'static>(
    app: AppHandle,
    cell: String,
    request_id: Option<crate::js::RequestId>,
    timeout: Duration,
    handler: impl FnOnce(GuestStorage) -> F + Send + 'static,
) -> Result<GuestCompletion<T>> {
    invoke_target(
        app,
        cell,
        None,
        request_id,
        execution_deadline_ms(timeout)?,
        None,
        None,
        None,
        move |access, _reply| async move { handler(access).await.map(Some) },
    )
    .await
}

#[cfg(test)]
pub(crate) async fn invoke_scoped<
    T: Send + 'static,
    F: Future<Output = Result<Option<T>>> + Send + 'static,
>(
    app: AppHandle,
    resource: ResourceAddress,
    class: String,
    name: String,
    request_id: Option<crate::js::RequestId>,
    timeout: Duration,
    handler: impl FnOnce(GuestStorage, oneshot::Sender<Result<T>>) -> F + Send + 'static,
) -> Result<GuestCompletion<T>> {
    let cell = resource.cell_scope(&class, &name)?;
    let object = ObjectTarget {
        class,
        id: cell
            .split_once(':')
            .expect("validated object scope")
            .1
            .into(),
        name: Some(name),
    };
    invoke_object(
        app,
        resource,
        object,
        request_id,
        execution_deadline_ms(timeout)?,
        None,
        handler,
    )
    .await
}
