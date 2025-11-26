import { useEffect, useMemo, useState } from "react";
import {
  MapContainer,
  TileLayer,
  CircleMarker,
  Popup,
  useMap,
  useMapEvents,
  Marker,
} from "react-leaflet";
import L from "leaflet";

export interface StorePoint {
  location_id: string;
  name: string;
  chain: string | null;
  city: string | null;
  state: string | null;
  latitude: number | null;
  longitude: number | null;

  // Optional price fields (attached in App.tsx)
  regular_price?: number | null;
  promo_price?: number | null;
  price_date?: string | null;
}

interface PriceMapProps {
  stores: StorePoint[];
}

// Default view (rough center of continental US)
const DEFAULT_CENTER: [number, number] = [39.8283, -98.5795];
const DEFAULT_ZOOM = 4;

// Helper component to track zoom changes and expose the map instance
function ZoomTracker({
  onZoomChange,
  onMapReady,
}: {
  onZoomChange: (zoom: number) => void;
  onMapReady: (map: L.Map) => void;
}) {
  const map = useMapEvents({
    zoomend: (e) => {
      const mapZoom = e.target.getZoom();
      onZoomChange(mapZoom);
    },
  });

  // Give parent access to the map instance once
  useEffect(() => {
    onMapReady(map);
  }, [map, onMapReady]);

  return null;
}

// Cluster type
interface Cluster {
  lat: number;
  lng: number;
  stores: StorePoint[];
}

// Create a circular DivIcon for clusters
function makeClusterIcon(count: number): L.DivIcon {
  // Size grows with count but is capped
  const size = Math.min(70, 26 + Math.sqrt(count) * 7.5);

  const html = `
    <div style="
      background: rgba(0, 123, 255, 0.9);
      border-radius: 50%;
      width: ${size}px;
      height: ${size}px;
      display: flex;
      align-items: center;
      justify-content: center;
      color: white;
      font-weight: 600;
      border: 2px solid white;
      box-shadow: 0 0 6px rgba(0, 0, 0, 0.4);
      font-size: 12px;
    ">
      ${count}
    </div>
  `;

  return L.divIcon({
    html,
    className: "",
    iconSize: [size, size],
  });
}

// Marker for a cluster: circular icon with count, zooms in on click (no popup)
function ClusterMarker({ cluster }: { cluster: Cluster }) {
  const map = useMap();
  const count = cluster.stores.length;

  const handleClick = () => {
    const currentZoom = map.getZoom();
    const targetZoom = Math.min(13, currentZoom + 2); // zoom in a bit more, cap at 13
    map.setView([cluster.lat, cluster.lng], targetZoom);
  };

  return (
    <Marker
      position={[cluster.lat, cluster.lng]}
      icon={makeClusterIcon(count)}
      eventHandlers={{ click: handleClick }}
    />
  );
}

// Helper to interpolate between two colors (hex) for price mapping
function lerpColor(startHex: string, endHex: string, t: number): string {
  const clampT = Math.max(0, Math.min(1, t));
  const sh = startHex.replace("#", "");
  const eh = endHex.replace("#", "");

  const sr = parseInt(sh.slice(0, 2), 16);
  const sg = parseInt(sh.slice(2, 4), 16);
  const sb = parseInt(sh.slice(4, 6), 16);

  const er = parseInt(eh.slice(0, 2), 16);
  const eg = parseInt(eh.slice(2, 4), 16);
  const eb = parseInt(eh.slice(4, 6), 16);

  const r = Math.round(sr + (er - sr) * clampT);
  const g = Math.round(sg + (eg - sg) * clampT);
  const b = Math.round(sb + (eb - sb) * clampT);

  const hr = r.toString(16).padStart(2, "0");
  const hg = g.toString(16).padStart(2, "0");
  const hb = b.toString(16).padStart(2, "0");

  return `#${hr}${hg}${hb}`;
}

export function PriceMap({ stores }: PriceMapProps) {
  // Filter out stores with missing coordinates
  const points = useMemo(
    () => stores.filter((s) => s.latitude !== null && s.longitude !== null),
    [stores]
  );

  const [zoom, setZoom] = useState<number>(DEFAULT_ZOOM);
  const [mapInstance, setMapInstance] = useState<L.Map | null>(null);

  // Compute min/max price across all points (using promo if available, otherwise regular)
  const priceRange = useMemo(() => {
    const prices: number[] = [];
    for (const s of points) {
      const val =
        typeof s.promo_price === "number"
          ? s.promo_price
          : typeof s.regular_price === "number"
          ? s.regular_price
          : null;
      if (val !== null) prices.push(val);
    }
    if (prices.length === 0) {
      return { min: null as number | null, max: null as number | null };
    }
    return {
      min: Math.min(...prices),
      max: Math.max(...prices),
    };
  }, [points]);

  // Map price to a color from green (low) to red (high)
  function getPriceColor(store: StorePoint): string {
    const basePrice =
      typeof store.promo_price === "number"
        ? store.promo_price
        : typeof store.regular_price === "number"
        ? store.regular_price
        : null;

    if (
      basePrice === null ||
      priceRange.min === null ||
      priceRange.max === null ||
      priceRange.max === priceRange.min
    ) {
      // Fallback color if no price or no range
      return "#007bff";
    }

    const t = (basePrice - priceRange.min) / (priceRange.max - priceRange.min);
    // green (#2ecc71) to red (#e74c3c)
    return lerpColor("#2ecc71", "#e74c3c", t);
  }

  // Decide when to show clusters vs. individual points.
  // We'll cluster when zoomed OUT (zoom < 11), and show individuals when zoomed IN (zoom >= 11).
  const { clusters, individualPoints } = useMemo(() => {
    if (points.length === 0) {
      return {
        clusters: [] as Cluster[],
        individualPoints: [] as StorePoint[],
      };
    }

    if (zoom >= 11) {
      // Zoomed in enough: show individual stores only
      return {
        clusters: [] as Cluster[],
        individualPoints: points,
      };
    }

    // Zoomed out: cluster stores into grid cells
    // Grid size depends on zoom: more coarse when zoomed out further
    let gridSize: number;
    if (zoom <= 4) {
      gridSize = 6; // very coarse at national view
    } else if (zoom <= 7) {
      gridSize = 3.5; // medium
    } else {
      gridSize = 1.5; // finer as we zoom in
    }

    const clusterMap = new Map<string, Cluster>();

    for (const s of points) {
      const lat = s.latitude as number;
      const lng = s.longitude as number;

      const keyLat = Math.round(lat / gridSize) * gridSize;
      const keyLng = Math.round(lng / gridSize) * gridSize;
      const key = `${keyLat.toFixed(2)}_${keyLng.toFixed(2)}`;

      const existing = clusterMap.get(key);
      if (!existing) {
        clusterMap.set(key, {
          lat,
          lng,
          stores: [s],
        });
      } else {
        existing.stores.push(s);
        // Update center as simple running average
        const n = existing.stores.length;
        existing.lat = existing.lat + (lat - existing.lat) / n;
        existing.lng = existing.lng + (lng - existing.lng) / n;
      }
    }

    const clusters = Array.from(clusterMap.values());
    return {
      clusters,
      individualPoints: [] as StorePoint[],
    };
  }, [points, zoom]);

  if (points.length === 0) {
    return <div>No store locations with coordinates to display.</div>;
  }

  const handleResetZoom = () => {
    if (mapInstance) {
      mapInstance.setView(DEFAULT_CENTER, DEFAULT_ZOOM);
    }
  };

  return (
    <div style={{ position: "relative", height: "100vh", width: "100%" }}>
      <MapContainer
        center={DEFAULT_CENTER}
        zoom={zoom}
        style={{ height: "100%", width: "100%" }}
      >
        {/* Track zoom changes & get map instance */}
        <ZoomTracker onZoomChange={setZoom} onMapReady={setMapInstance} />

        <TileLayer
          attribution='&copy; OpenStreetMap contributors'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />

        {/* Cluster markers when zoomed out */}
        {clusters.map((cluster, idx) => (
          <ClusterMarker key={`cluster-${idx}`} cluster={cluster} />
        ))}

        {/* Individual store markers when zoomed in */}
        {individualPoints.map((s) => (
          <CircleMarker
            key={s.location_id}
            center={[s.latitude as number, s.longitude as number]}
            radius={9} // larger pins
            pathOptions={{
              color: getPriceColor(s),
              fillColor: getPriceColor(s),
              fillOpacity: 0.9,
            }}
          >
            <Popup>
              <div>
                <strong>{s.name}</strong>
                <br />
                {s.chain && (
                  <>
                    {s.chain}
                    <br />
                  </>
                )}
                {s.city}, {s.state}
                <br />
                Location ID: {s.location_id}
                <br />
                {typeof s.promo_price === "number" ||
                typeof s.regular_price === "number" ? (
                  <>
                    <br />
                    {typeof s.regular_price === "number" && (
                      <>
                        Regular: ${s.regular_price.toFixed(2)}
                        <br />
                      </>
                    )}
                    {typeof s.promo_price === "number" && (
                      <>
                        Promo: ${s.promo_price.toFixed(2)}
                        <br />
                      </>
                    )}
                    {s.price_date && (
                      <>
                        Date: {s.price_date}
                        <br />
                      </>
                    )}
                  </>
                ) : (
                  <>
                    <br />
                    No price data for this product at this store.
                  </>
                )}
              </div>
            </Popup>
          </CircleMarker>
        ))}
      </MapContainer>

      {/* Reset Zoom button overlay (bottom-right) */}
      <button
        onClick={handleResetZoom}
        style={{
          position: "absolute",
          right: "10px",
          bottom: "10px",
          zIndex: 1000,
          padding: "0.4rem 0.6rem",
          borderRadius: "4px",
          border: "1px solid #ccc",
          backgroundColor: "white",
          cursor: "pointer",
          fontSize: "0.85rem",
        }}
      >
        Reset Zoom
      </button>
    </div>
  );
}
