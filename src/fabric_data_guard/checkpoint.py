import great_expectations as gx


def create_checkpoint(fabric_data_guard, **kwargs):
    """
    Creates a checkpoint for validations with optional notifications.

    Args:
        fabric_data_guard (FabricDataGuard): The FabricDataGuard instance.
        slack_notification (bool): Whether to enable Slack notifications.
        email_notification (bool): Whether to enable email notifications.
        teams_notification (bool): Whether to enable Microsoft Teams notifications.
        **kwargs: Additional keyword arguments for customizing notifications.

    Returns:
        gx.Checkpoint: The created or retrieved checkpoint.
    """
    checkpoint_name = f"{fabric_data_guard.datasource_name}AnalysisCheckpoint"

    # Base actions for the checkpoint
    actions = [gx.checkpoint.UpdateDataDocsAction(name="update_all_data_docs")]

    # Add Slack notification action if enabled
    if kwargs.get("slack_notification", False):
        actions.append(
            gx.checkpoint.SlackNotificationAction(
                name="send_slack_notification_on_failed_expectations",
                slack_token=kwargs.get("slack_token"),
                slack_channel=kwargs.get("slack_channel"),
                notify_on=kwargs.get("notify_on", "failure"),
                show_failed_expectations=kwargs.get("show_failed_expectations", True),
            )
        )

    # Add Email notification action if enabled
    if kwargs.get("email_notification", False):
        actions.append(
            gx.checkpoint.EmailAction(
                name="send_email_notification_on_validation_result",
                smtp_address=kwargs.get("smtp_address", "smtp.example.com"),
                smtp_port=kwargs.get("smtp_port", 587),
                sender_login=kwargs.get("sender_login", "your_email@example.com"),
                sender_password=kwargs.get("sender_password", "your_password"),
                receiver_emails=kwargs.get("receiver_emails", "recipient@example.com"),
                use_tls=kwargs.get("use_tls", True),
                notify_on=kwargs.get("notify_on", "failure"),
            )
        )

    # Add Microsoft Teams notification action if enabled
    if kwargs.get("teams_notification", False):
        actions.append(
            gx.checkpoint.MicrosoftTeamsNotificationAction(
                name="send_microsoft_teams_notification_on_validation_result",
                teams_webhook=kwargs.get("teams_webhook"),
                notify_on=kwargs.get("notify_on", "failure"),
            )
        )

    # Create or retrieve the checkpoint with the defined actions
    checkpoint = (
        fabric_data_guard.context.checkpoints.add(
            gx.Checkpoint(
                name=checkpoint_name,
                validation_definitions=[fabric_data_guard.validation_definition],
                actions=actions,
                result_format={
                    "result_format": "COMPLETE",
                    "unexpected_index_column_names": kwargs.get(
                        "unexpected_identifiers"
                    ),
                },
            )
        )
        if not any(
            cs.name == checkpoint_name
            for cs in fabric_data_guard.context.checkpoints.all()
        )
        else fabric_data_guard.context.checkpoints.get(checkpoint_name)
    )

    return checkpoint
