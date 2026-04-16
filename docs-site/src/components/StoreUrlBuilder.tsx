import { useState } from 'react';

type Backend =
  | { kind: 'neptune-db'; cluster: string; region: string }
  | { kind: 'neptune-graph'; graphId: string }
  | { kind: 'neo4j'; host: string; port: string }
  | { kind: 'falkordb'; host: string; port: string };

const backends = [
  { id: 'neptune-db', label: 'Neptune DB' },
  { id: 'neptune-graph', label: 'Neptune Analytics' },
  { id: 'neo4j', label: 'Neo4j' },
  { id: 'falkordb', label: 'FalkorDB' },
] as const;

function buildUrl(b: Backend): string {
  switch (b.kind) {
    case 'neptune-db':
      return `neptune-db://${b.cluster}.cluster-xxxxxxxx.${b.region}.neptune.amazonaws.com`;
    case 'neptune-graph':
      return `neptune-graph://${b.graphId}`;
    case 'neo4j':
      return `neo4j://${b.host}:${b.port}`;
    case 'falkordb':
      return `falkordb://${b.host}:${b.port}`;
  }
}

const inputStyle: React.CSSProperties = {
  padding: '0.5rem 0.75rem',
  background: 'var(--sl-color-bg-inline-code)',
  color: 'var(--sl-color-white)',
  border: '1px solid var(--sl-color-gray-5)',
  borderRadius: '6px',
  fontSize: '0.9rem',
  fontFamily: 'var(--sl-font-system-mono)',
  width: '100%',
};

const labelStyle: React.CSSProperties = {
  display: 'block',
  fontSize: '0.8rem',
  color: 'var(--sl-color-gray-2)',
  marginBottom: '0.25rem',
  textTransform: 'uppercase',
  letterSpacing: '0.05em',
};

export default function StoreUrlBuilder() {
  const [kind, setKind] = useState<Backend['kind']>('neptune-db');
  const [cluster, setCluster] = useState('my-graph');
  const [region, setRegion] = useState('us-east-1');
  const [graphId, setGraphId] = useState('g-abc123def456');
  const [host, setHost] = useState('localhost');
  const [port, setPort] = useState('7687');

  let backend: Backend;
  if (kind === 'neptune-db') backend = { kind, cluster, region };
  else if (kind === 'neptune-graph') backend = { kind, graphId };
  else if (kind === 'neo4j') backend = { kind, host, port };
  else backend = { kind, host, port };

  const url = buildUrl(backend);

  return (
    <div
      style={{
        padding: '1.25rem',
        background: 'linear-gradient(180deg, rgba(124,58,237,0.06), rgba(124,58,237,0.02))',
        border: '1px solid var(--sl-color-gray-5)',
        borderRadius: '12px',
        marginBlock: '1.5rem',
      }}
    >
      <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap', marginBottom: '1rem' }}>
        {backends.map((b) => (
          <button
            key={b.id}
            onClick={() => setKind(b.id)}
            style={{
              padding: '0.4rem 0.9rem',
              borderRadius: '999px',
              border: '1px solid var(--sl-color-gray-5)',
              background:
                kind === b.id
                  ? 'linear-gradient(135deg, #7c3aed 0%, #4f46e5 100%)'
                  : 'transparent',
              color: kind === b.id ? '#fff' : 'var(--sl-color-gray-2)',
              cursor: 'pointer',
              fontSize: '0.85rem',
              fontWeight: 500,
              transition: 'all 0.15s',
            }}
          >
            {b.label}
          </button>
        ))}
      </div>

      <div
        style={{
          display: 'grid',
          gap: '0.75rem',
          gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))',
          marginBottom: '1rem',
        }}
      >
        {kind === 'neptune-db' && (
          <>
            <div>
              <label style={labelStyle}>Cluster name</label>
              <input style={inputStyle} value={cluster} onChange={(e) => setCluster(e.target.value)} />
            </div>
            <div>
              <label style={labelStyle}>Region</label>
              <input style={inputStyle} value={region} onChange={(e) => setRegion(e.target.value)} />
            </div>
          </>
        )}
        {kind === 'neptune-graph' && (
          <div>
            <label style={labelStyle}>Graph ID</label>
            <input style={inputStyle} value={graphId} onChange={(e) => setGraphId(e.target.value)} />
          </div>
        )}
        {(kind === 'neo4j' || kind === 'falkordb') && (
          <>
            <div>
              <label style={labelStyle}>Host</label>
              <input style={inputStyle} value={host} onChange={(e) => setHost(e.target.value)} />
            </div>
            <div>
              <label style={labelStyle}>Port</label>
              <input style={inputStyle} value={port} onChange={(e) => setPort(e.target.value)} />
            </div>
          </>
        )}
      </div>

      <div>
        <label style={labelStyle}>Connection string</label>
        <code
          style={{
            display: 'block',
            padding: '0.75rem 1rem',
            background: 'var(--sl-color-bg-inline-code)',
            border: '1px solid var(--sl-color-accent)',
            borderRadius: '8px',
            fontSize: '0.9rem',
            wordBreak: 'break-all',
            color: 'var(--sl-color-accent-high)',
          }}
        >
          {url}
        </code>
      </div>
    </div>
  );
}
