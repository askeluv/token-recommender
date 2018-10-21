select
receivers.to_address as address,
receivers.token_address,
received_amount - coalesce(sent_amount, 0) as balance
from

-- aggregate total amount of tokens received
(select
  token_address,
  to_address,
  sum(cast(value as FLOAT64) / POW(10, cast(tokens.decimals as INT64))) as received_amount
from `bigquery-public-data.ethereum_blockchain.token_transfers` as tr
join `bigquery-public-data.ethereum_blockchain.tokens` as tokens
on tr.token_address = tokens.address
where tr.block_timestamp < '2018-09-01'
group by token_address, to_address) as receivers
left join

-- aggregate total amount of tokens sent
(select
  token_address,
  from_address,
  sum(cast(value as FLOAT64) / POW(10, cast(tokens.decimals as INT64))) as sent_amount
from `bigquery-public-data.ethereum_blockchain.token_transfers` as tr
join `bigquery-public-data.ethereum_blockchain.tokens` as tokens
on tr.token_address = tokens.address
where tr.block_timestamp < '2018-09-01'
group by token_address, from_address) as senders
on receivers.token_address = senders.token_address
and receivers.to_address = senders.from_address
where received_amount - coalesce(sent_amount, 0) > 0.0;