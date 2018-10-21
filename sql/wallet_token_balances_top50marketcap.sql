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
where received_amount - coalesce(sent_amount, 0) > 0.0
and receivers.token_address in (
'0xf230b790e05390fc8295f4d3f60332c93bed42e2',
'0xb8c77482e45f1f44de1745f52c74426c631bdd52',
'0xd850942ef8811f2a866692a623011bde52a462c1',
'0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2',
'0xe41d2489571d322189246dafa5ebde1f4699f498',
'0xd26114cd6ee289accf82350c8d8487fedb8a0c07',
'0x5ca9a71b1d01849c0a95490cc00559717fcf0d1d',
'0xb5a5f22694352c15b00323844ad545abb2b11028',
'0x05f4a42e251f2d52b8ed15e9fedaacfcef1fad27',
'0x0d8775f648430679a709e98d2b0cb6250d2887ef',
'0xcb97e65f07da24d46bcdd078ebebd7c6e6e3d750',
'0x358d12436080a01a16f711014610f8a4c2c2d233',
'0xa15c7ebe1f07caf6bff097d8a589fb8ac49ae5b3',
'0x8dd5fbce2f6a956c3022ba3663759011dd51e73e',
'0xa74476443119a942de498590fe1f2454d7d4ac0d',
'0xe94327d07fc17907b4db788e5adf2ed424addff6',
'0x514910771af9ca656af840dff83e8264ecf986ca',
'0xd4fa1460f537bb9085d22c7bccb5dd450ef28e3a',
'0x744d70fdbe2ba4cf95131626614a1763df805b9e',
'0xb7cb1c96db6b22b0d3d9536e0108d062bd488f74',
'0x4ceda7906a5ed2179785cd3a40a69ee8bc99c466',
'0xfa1a856cfa3409cfa145fa4e20eb270df3eb21ab',
'0x039b5649a59967e3e936d7471f9c3700100ee1ab',
'0x1c83501478f1320977047008496dacbd60bb15ef',
'0xe0b7927c4af23765cb51314a0e0521a9645f0e2a',
'0xf85feea2fdd81d51177f6b8f35f0e6734ce45f5f',
'0x9ab165d795019b6d8b3e971dda91071421305e5a',
'0xef68e7c694f40c8202821edf525de3782458639f',
'0xbf2179859fc6d5bee9bf9158632dc51678a4100e',
'0x6f259637dcd74c767781e37bc6133cd6a68aa161',
'0x419d0d8bdd9af5e606ae2232ed285aff190e711b',
'0x5d65d971895edc438f465c17db6992698a52318d',
'0xa4e8c3ec456107ea67d3075bf9e3df3a75823db0',
'0x618e75ac90b12c6049ba3b27f5d5f8651b0037f6',
'0x0f5d2fb29fb7d3cfee444a200298f468908cc942',
'0x1f573d6fb3f13d689ff844b4ce37794d79a7ff1c',
'0x08d32b0da63e2c3bcf8019c9c5d849d7a9d791e6',
'0xcbce61316759d807c474441952ce41985bbc5a40',
'0x595832f8fc6bf59c85c527fec3740a1b7a361269',
'0x4672bad527107471cb5067a887f4656d585a8a31',
'0x39bb259f66e1c59d5abef88375979b4d20d98022',
'0x9992ec3cf6a55b00978cddf2b27bc6882d88d1ec',
'0xb62132e35a6c13ee1ee0f84dc5d40bad8d815206',
'0x89d24a6b4ccb1b6faa2625fe562bdd9a23260359',
'0x8f3470a7388c05ee4e7af3d01d8c722b0ff52374',
'0x3883f5e181fccaf8410fa61e12b59bad963fb645',
'0xb97048628db6b661d4c2aa833e95dbe1a905b280',
'0x103c3a209da59d3e7c4a89307e66521e081cfdf0',
'0x4156d3342d5c385a87d264f90653733592000581',
'0xdd974d5c2e2928dea5f71b9825b8b646686bd200'
);
