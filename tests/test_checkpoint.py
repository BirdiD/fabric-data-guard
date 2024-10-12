from unittest.mock import Mock, patch

import great_expectations as gx

from src.fabric_data_guard.checkpoint import create_checkpoint


def test_create_checkpoint_basic(mock_fabric_data_guard):
    with patch("great_expectations.Checkpoint") as mock_checkpoint:
        # Configure the mock to return a new checkpoint
        mock_fabric_data_guard.context.checkpoints.add.return_value = (
            mock_checkpoint.return_value
        )

        checkpoint = create_checkpoint(mock_fabric_data_guard)

        assert checkpoint is not None
        mock_fabric_data_guard.context.checkpoints.add.assert_called_once()
        mock_checkpoint.assert_called_once()

        call_kwargs = mock_checkpoint.call_args[1]
        assert (
            call_kwargs["name"]
            == f"{mock_fabric_data_guard.datasource_name}AnalysisCheckpoint"
        )
        assert call_kwargs["validation_definitions"] == [
            mock_fabric_data_guard.validation_definition
        ]
        assert len(call_kwargs["actions"]) == 1
        assert isinstance(call_kwargs["actions"][0], gx.checkpoint.UpdateDataDocsAction)
        assert call_kwargs["result_format"] == {
            "result_format": "COMPLETE",
            "unexpected_index_column_names": None,
        }


def test_create_checkpoint_with_slack(mock_fabric_data_guard):
    with patch("great_expectations.Checkpoint") as mock_checkpoint:
        mock_fabric_data_guard.context.checkpoints.add.return_value = (
            mock_checkpoint.return_value
        )
        checkpoint = create_checkpoint(
            mock_fabric_data_guard,
            slack_notification=True,
            slack_token="test_token",
            slack_channel="test_channel",
        )

        call_kwargs = mock_checkpoint.call_args[1]
        actions = call_kwargs["actions"]
        assert len(actions) == 2
        assert isinstance(actions[1], gx.checkpoint.SlackNotificationAction)
        assert actions[1].slack_token == "test_token"
        assert actions[1].slack_channel == "test_channel"


def test_create_checkpoint_with_email(mock_fabric_data_guard):
    with patch("great_expectations.Checkpoint") as mock_checkpoint:
        mock_fabric_data_guard.context.checkpoints.add.return_value = (
            mock_checkpoint.return_value
        )
        checkpoint = create_checkpoint(
            mock_fabric_data_guard,
            email_notification=True,
            sender_login="test@example.com",
        )

        call_kwargs = mock_checkpoint.call_args[1]
        actions = call_kwargs["actions"]
        assert len(actions) == 2
        assert isinstance(actions[1], gx.checkpoint.EmailAction)
        assert actions[1].sender_login == "test@example.com"


def test_create_checkpoint_with_teams(mock_fabric_data_guard):
    with patch("great_expectations.Checkpoint") as mock_checkpoint:
        mock_fabric_data_guard.context.checkpoints.add.return_value = (
            mock_checkpoint.return_value
        )
        checkpoint = create_checkpoint(
            mock_fabric_data_guard,
            teams_notification=True,
            teams_webhook="test_webhook",
        )

        call_kwargs = mock_checkpoint.call_args[1]
        actions = call_kwargs["actions"]
        assert len(actions) == 2
        assert isinstance(actions[1], gx.checkpoint.MicrosoftTeamsNotificationAction)
        assert actions[1].teams_webhook == "test_webhook"


def test_create_checkpoint_with_all_notifications(mock_fabric_data_guard):
    with patch("great_expectations.Checkpoint") as mock_checkpoint:
        mock_fabric_data_guard.context.checkpoints.add.return_value = (
            mock_checkpoint.return_value
        )
        checkpoint = create_checkpoint(
            mock_fabric_data_guard,
            slack_notification=True,
            email_notification=True,
            teams_notification=True,
            slack_token="test_token",
            slack_channel="test_channel",
            teams_webhook="test_webhook",
        )

        call_kwargs = mock_checkpoint.call_args[1]
        actions = call_kwargs["actions"]
        assert len(actions) == 4
        assert isinstance(actions[1], gx.checkpoint.SlackNotificationAction)
        assert isinstance(actions[2], gx.checkpoint.EmailAction)
        assert isinstance(actions[3], gx.checkpoint.MicrosoftTeamsNotificationAction)


def test_create_checkpoint_with_unexpected_identifiers(mock_fabric_data_guard):
    with patch("great_expectations.Checkpoint") as mock_checkpoint:
        mock_fabric_data_guard.context.checkpoints.add.return_value = (
            mock_checkpoint.return_value
        )
        unexpected_identifiers = ["id1", "id2"]
        checkpoint = create_checkpoint(
            mock_fabric_data_guard, unexpected_identifiers=unexpected_identifiers
        )

        call_kwargs = mock_checkpoint.call_args[1]
        result_format = call_kwargs["result_format"]
        assert result_format["unexpected_index_column_names"] == unexpected_identifiers
