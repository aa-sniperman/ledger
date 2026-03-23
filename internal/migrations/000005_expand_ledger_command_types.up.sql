ALTER TABLE ledger.ledger_commands
DROP CONSTRAINT IF EXISTS ledger_commands_command_type_check;

ALTER TABLE ledger.ledger_commands
ADD CONSTRAINT ledger_commands_command_type_check
CHECK (
    command_type IN (
        'transactions.create',
        'transactions.post',
        'transactions.archive',
        'payments.withdrawals.create',
        'payments.withdrawals.post',
        'payments.withdrawals.archive',
        'payments.deposits.record'
    )
);
