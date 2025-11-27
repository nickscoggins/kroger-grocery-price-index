import { useEffect, useMemo, useState } from "react";
import { supabase } from "./lib/supabaseClient";
import { PriceMap } from "./components/PriceMap";
import type { StorePoint } from "./components/PriceMap";

interface Product {
  upc: string;
  description: string | null;
  brand: string | null;
  categories: string | null;
}

interface StorePriceRow {
  location_id: string; // string to match DB
  regular_price: number | null;
  promo_price: number | null;
  price_date: string;
}

function App() {
  const [stores, setStores] = useState<StorePoint[]>([]);
  const [loadingStores, setLoadingStores] = useState(true);

  const [products, setProducts] = useState<Product[]>([]);
  const [loadingProducts, setLoadingProducts] = useState(true);

  const [isSidebarOpen, setIsSidebarOpen] = useState(true);

  // Filter state
  const [selectedProductUpc, setSelectedProductUpc] = useState<string>(""); // default: no product selected
  const [selectedRegion, setSelectedRegion] = useState<string>("ALL");
  const [selectedDivision, setSelectedDivision] = useState<string>("ALL");
  const [selectedState, setSelectedState] = useState<string>("ALL");
  const [selectedChain, setSelectedChain] = useState<string>("ALL");

  // Price data for the selected product
  const [productPrices, setProductPrices] = useState<StorePriceRow[]>([]);
  const [loadingPrices, setLoadingPrices] = useState(false);

  // Hover tooltip state for price markers on the scale
  const [hoverPriceInfo, setHoverPriceInfo] = useState<{
    price: number;
    count: number;
    position: number; // 0–100 (% along the scale)
  } | null>(null);

  const handleClearFilters = () => {
    setSelectedRegion("ALL");
    setSelectedDivision("ALL");
    setSelectedState("ALL");
    setSelectedChain("ALL");
    setSelectedProductUpc(""); // reset to no product selected
  };

  // Clear tooltip when product or filters change
  useEffect(() => {
    setHoverPriceInfo(null);
  }, [selectedProductUpc, selectedRegion, selectedDivision, selectedState, selectedChain]);

  // --- Fetch STORES (with paging to avoid 1000-row cap) ---
  useEffect(() => {
    async function fetchStores() {
      setLoadingStores(true);

      const pageSize = 1000;
      const ranges: [number, number][] = [
        [0, pageSize - 1],
        [pageSize, 2 * pageSize - 1],
        [2 * pageSize, 3 * pageSize - 1],
      ];

      try {
        const results = await Promise.all(
          ranges.map(([from, to]) =>
            supabase
              .from("stores")
              .select(
                "location_id, name, chain, city, state, latitude, longitude, census_region, census_division"
              )
              .range(from, to)
          )
        );

        const allData = results.flatMap((res) => res.data ?? []);

        const byId = new Map<string, StorePoint>();
        for (const row of allData as StorePoint[]) {
          byId.set(row.location_id, row);
        }

        const uniqueStores = Array.from(byId.values());
        setStores(uniqueStores);
      } catch (e) {
        console.error("Error fetching stores:", e);
        setStores([]);
      } finally {
        setLoadingStores(false);
      }
    }

    fetchStores();
  }, []);

  // --- Fetch PRODUCTS once on mount ---
  useEffect(() => {
    async function fetchProducts() {
      setLoadingProducts(true);

      const { data, error } = await supabase
        .from("products")
        .select("upc, description, brand, categories")
        .order("description", { ascending: true });

      if (error) {
        console.error("Error fetching products:", error);
        setProducts([]);
      } else {
        const productData = (data ?? []) as Product[];
        setProducts(productData);
      }

      setLoadingProducts(false);
    }

    fetchProducts();
  }, []);

  // --- Fetch latest prices per store for the selected product (from latest_prices) ---
  useEffect(() => {
    async function fetchPricesForProduct() {
      // If no product selected, clear prices and skip
      if (!selectedProductUpc) {
        setProductPrices([]);
        return;
      }

      setLoadingPrices(true);

      const pageSize = 1000;
      const ranges: [number, number][] = [
        [0, pageSize - 1], // 0–999
        [pageSize, 2 * pageSize - 1], // 1000–1999
        [2 * pageSize, 3 * pageSize - 1], // 2000–2999
      ];

      try {
        const results = await Promise.all(
          ranges.map(([from, to]) =>
            supabase
              .from("latest_prices")
              .select("location_id, price_date, regular_price, promo_price")
              .eq("upc", selectedProductUpc)
              .range(from, to)
          )
        );

        const allRows = results.flatMap((res) => res.data ?? []) as {
          location_id: string;
          price_date: string;
          regular_price: number | null;
          promo_price: number | null;
        }[];

        const latestByLocation = new Map<string, StorePriceRow>();

        for (const r of allRows) {
          if (!latestByLocation.has(r.location_id)) {
            latestByLocation.set(r.location_id, {
              location_id: r.location_id,
              price_date: r.price_date,
              regular_price: r.regular_price,
              promo_price: r.promo_price,
            });
          }
        }

        setProductPrices(Array.from(latestByLocation.values()));
      } catch (e) {
        console.error("Error fetching prices for product:", e);
        setProductPrices([]);
      } finally {
        setLoadingPrices(false);
      }
    }

    fetchPricesForProduct();
  }, [selectedProductUpc]);

  // --- Derive unique regions, divisions, states, chains from stores for filter dropdowns ---
  const uniqueRegions = useMemo(() => {
    const set = new Set<string>();
    stores.forEach((s) => {
      if (s.census_region) set.add(s.census_region);
    });
    return Array.from(set).sort();
  }, [stores]);

  const uniqueDivisions = useMemo(() => {
    const set = new Set<string>();
    stores.forEach((s) => {
      if (s.census_division) set.add(s.census_division);
    });
    return Array.from(set).sort();
  }, [stores]);

  const uniqueStates = useMemo(() => {
    const set = new Set<string>();
    stores.forEach((s) => {
      if (s.state) set.add(s.state);
    });
    return Array.from(set).sort();
  }, [stores]);

  const uniqueChains = useMemo(() => {
    const set = new Set<string>();
    stores.forEach((s) => {
      if (s.chain) set.add(s.chain);
    });
    return Array.from(set).sort();
  }, [stores]);

  // --- Apply location filters to stores for the map ---
  const filteredStores = useMemo(() => {
    return stores.filter((s) => {
      const matchesRegion =
        selectedRegion === "ALL" || s.census_region === selectedRegion;
      const matchesDivision =
        selectedDivision === "ALL" || s.census_division === selectedDivision;
      const matchesState =
        selectedState === "ALL" || s.state === selectedState;
      const matchesChain =
        selectedChain === "ALL" || s.chain === selectedChain;
      return matchesRegion && matchesDivision && matchesState && matchesChain;
    });
  }, [stores, selectedRegion, selectedDivision, selectedState, selectedChain]);

  // --- Attach prices to filtered stores ---
  const storesWithPrice = useMemo(() => {
    if (productPrices.length === 0) {
      // Either no product selected or no price data for this product
      return filteredStores.map((s) => ({
        ...s,
        regular_price: null as number | null,
        promo_price: null as number | null,
        price_date: null as string | null,
      }));
    }

    const priceByLocation = new Map<string, StorePriceRow>();
    productPrices.forEach((p) => {
      priceByLocation.set(p.location_id, p);
    });

    return filteredStores.map((s) => {
      const priceRow = priceByLocation.get(s.location_id);
      return {
        ...s,
        regular_price: priceRow?.regular_price ?? null,
        promo_price: priceRow?.promo_price ?? null,
        price_date: priceRow?.price_date ?? null,
      };
    });
  }, [filteredStores, productPrices]);

  // --- Final set of stores to show on the map ---
  const storesForMap = useMemo(() => {
    // If no product selected: show all filtered stores (even without price)
    if (!selectedProductUpc) {
      return storesWithPrice;
    }

    // If a product is selected: only show stores that have a price
    return storesWithPrice.filter(
      (s) =>
        typeof s.promo_price === "number" ||
        typeof s.regular_price === "number"
    );
  }, [storesWithPrice, selectedProductUpc]);

  // --- Price stats (min/median/max/mean + distribution of unique prices) ---
  const priceStats = useMemo(() => {
    if (!selectedProductUpc) return null;

    const values: number[] = [];
    const countsMap = new Map<number, number>(); // price -> count

    for (const s of storesForMap) {
      const base =
        typeof s.promo_price === "number"
          ? s.promo_price
          : typeof s.regular_price === "number"
          ? s.regular_price
          : null;

      if (base !== null) {
        // Round to 2 decimals to avoid tiny float differences
        const price = Math.round(base * 100) / 100;
        values.push(price);
        countsMap.set(price, (countsMap.get(price) ?? 0) + 1);
      }
    }

    if (values.length === 0) return null;

    values.sort((a, b) => a - b);
    const min = values[0];
    const max = values[values.length - 1];
    const mid = Math.floor(values.length / 2);
    const median =
      values.length % 2 === 1
        ? values[mid]
        : (values[mid - 1] + values[mid]) / 2;

    const sum = values.reduce((acc, v) => acc + v, 0);
    const mean = sum / values.length;

    const distribution = Array.from(countsMap.entries())
      .map(([price, count]) => ({ price, count }))
      .sort((a, b) => a.price - b.price);

    return {
      min,
      median,
      max,
      mean,
      count: values.length,
      distribution,
    };
  }, [storesForMap, selectedProductUpc]);

  return (
    <div
      style={{
        display: "flex",
        height: "100vh", // lock app height to viewport
        width: "100vw",
        overflow: "hidden",
        fontFamily: "system-ui, -apple-system, BlinkMacSystemFont, sans-serif",
      }}
    >
      {/* Side panel (scrollable) */}
      {isSidebarOpen && (
        <div
          style={{
            width: "320px",
            minWidth: "260px",
            maxWidth: "400px",
            borderRight: "1px solid #ddd",
            padding: "1rem",
            boxSizing: "border-box",
            backgroundColor: "#fafafa",
            display: "flex",
            flexDirection: "column",
            height: "100%",
            overflowY: "auto",
          }}
        >
          <h1 style={{ fontSize: "1.25rem", marginBottom: "0.5rem" }}>
            Kroger Price Map
          </h1>

          <p style={{ marginTop: 0, marginBottom: "0.75rem", color: "#555" }}>
            Explore store locations and filter by product and region.
          </p>

          {/* Basic stats */}
          <div
            style={{
              marginBottom: "1rem",
              fontSize: "0.9rem",
              color: "#444",
            }}
          >
            {loadingStores ? (
              <p>Loading stores…</p>
            ) : (
              <p style={{ margin: 0 }}>
                Stores loaded: {stores.length} (showing {storesForMap.length}{" "}
                after filters)
              </p>
            )}
            {loadingProducts ? (
              <p>Loading products…</p>
            ) : (
              <p style={{ margin: 0 }}>Products loaded: {products.length}</p>
            )}
            {selectedProductUpc ? (
              <p style={{ margin: 0, fontSize: "0.8rem", color: "#777" }}>
                {loadingPrices
                  ? "Loading prices for selected product…"
                  : `Price rows loaded: ${productPrices.length}`}
              </p>
            ) : (
              <p style={{ margin: 0, fontSize: "0.8rem", color: "#777" }}>
                No product selected — showing all stores.
              </p>
            )}
          </div>

          {/* Product selector */}
          <div style={{ marginBottom: "0.75rem" }}>
            <label
              style={{
                display: "block",
                marginBottom: "0.25rem",
                fontWeight: 500,
              }}
            >
              Product
            </label>
            {loadingProducts ? (
              <div>Loading products…</div>
            ) : (
              <select
                value={selectedProductUpc}
                onChange={(e) => setSelectedProductUpc(e.target.value)}
                style={{
                  width: "100%",
                  padding: "0.4rem 0.6rem",
                  borderRadius: "4px",
                  border: "1px solid #ccc",
                  fontSize: "0.9rem",
                }}
              >
                <option value="">
                  (No product selected – show all stores)
                </option>
                {products.map((p) => (
                  <option key={p.upc} value={p.upc}>
                    {p.brand ? `${p.brand} – ` : ""}
                    {p.description || "(No description)"} ({p.upc})
                  </option>
                ))}
              </select>
            )}
          </div>

          {/* Price summary card for selected product */}
          {selectedProductUpc && priceStats && (
            <div
              style={{
                marginBottom: "1rem",
                padding: "0.75rem",
                borderRadius: "6px",
                border: "1px solid #ddd",
                backgroundColor: "#fff",
                fontSize: "0.85rem",
                color: "#444",
              }}
            >
              <div
                style={{
                  fontWeight: 600,
                  marginBottom: "0.25rem",
                }}
              >
                Price summary (selected product)
              </div>
              <div
                style={{
                  display: "flex",
                  justifyContent: "space-between",
                  marginBottom: "0.4rem",
                }}
              >
                <span>Min: ${priceStats.min.toFixed(2)}</span>
                <span>Median: ${priceStats.median.toFixed(2)}</span>
                <span>Max: ${priceStats.max.toFixed(2)}</span>
              </div>

              {/* Gradient bar + interactive price markers */}
              <div
                style={{
                  position: "relative",
                  marginBottom: "0.25rem",
                  paddingTop: "4px",
                }}
              >
                <div
                  style={{
                    height: "10px",
                    borderRadius: "999px",
                    background:
                      "linear-gradient(to right, #2ecc71, #e67e22, #e74c3c)",
                  }}
                />

                {priceStats.distribution.map((bucket) => {
                  if (priceStats.max === priceStats.min) {
                    return null;
                  }

                  const t =
                    (bucket.price - priceStats.min) /
                    (priceStats.max - priceStats.min);
                  const position = Math.max(0, Math.min(1, t)) * 100;

                  return (
                    <div
                      key={bucket.price}
                      onMouseEnter={() =>
                        setHoverPriceInfo({
                          price: bucket.price,
                          count: bucket.count,
                          position,
                        })
                      }
                      onMouseLeave={() => setHoverPriceInfo(null)}
                      style={{
                        position: "absolute",
                        bottom: 0,
                        left: `${position}%`,
                        transform: "translateX(-50%)",
                        width: "2px",
                        height: "14px",
                        backgroundColor: "rgba(0, 0, 0, 0.65)",
                        cursor: "pointer",
                      }}
                    />
                  );
                })}

                {hoverPriceInfo && priceStats.max !== priceStats.min && (
                  <div
                    style={{
                      position: "absolute",
                      bottom: "20px",
                      left: `${hoverPriceInfo.position}%`,
                      transform: "translateX(-50%)",
                      padding: "2px 6px",
                      borderRadius: "4px",
                      backgroundColor: "rgba(0, 0, 0, 0.8)",
                      color: "#fff",
                      fontSize: "0.7rem",
                      whiteSpace: "nowrap",
                      pointerEvents: "none",
                      zIndex: 10,
                    }}
                  >
                    ${hoverPriceInfo.price.toFixed(2)} –{" "}
                    {hoverPriceInfo.count} store
                    {hoverPriceInfo.count === 1 ? "" : "s"}
                  </div>
                )}
              </div>

              <div
                style={{
                  textAlign: "center",
                  fontSize: "0.75rem",
                  color: "#555",
                  marginBottom: "0.2rem",
                }}
              >
                Avg: ${priceStats.mean.toFixed(2)}
              </div>

              <div
                style={{
                  marginTop: "0.15rem",
                  fontSize: "0.75rem",
                  color: "#777",
                }}
              >
                Based on {priceStats.count} stores with prices.
              </div>
            </div>
          )}

          {/* Region filter */}
          <div style={{ marginBottom: "0.75rem" }}>
            <label
              style={{
                display: "block",
                marginBottom: "0.25rem",
                fontWeight: 500,
              }}
            >
              Census Region
            </label>
            <select
              value={selectedRegion}
              onChange={(e) => setSelectedRegion(e.target.value)}
              style={{
                width: "100%",
                padding: "0.4rem 0.6rem",
                borderRadius: "4px",
                border: "1px solid #ccc",
                fontSize: "0.9rem",
              }}
            >
              <option value="ALL">All regions</option>
              {uniqueRegions.map((r) => (
                <option key={r} value={r}>
                  {r}
                </option>
              ))}
            </select>
          </div>

          {/* Division filter */}
          <div style={{ marginBottom: "0.75rem" }}>
            <label
              style={{
                display: "block",
                marginBottom: "0.25rem",
                fontWeight: 500,
              }}
            >
              Census Division
            </label>
            <select
              value={selectedDivision}
              onChange={(e) => setSelectedDivision(e.target.value)}
              style={{
                width: "100%",
                padding: "0.4rem 0.6rem",
                borderRadius: "4px",
                border: "1px solid #ccc",
                fontSize: "0.9rem",
              }}
            >
              <option value="ALL">All divisions</option>
              {uniqueDivisions.map((d) => (
                <option key={d} value={d}>
                  {d}
                </option>
              ))}
            </select>
          </div>

          {/* State filter */}
          <div style={{ marginBottom: "0.75rem" }}>
            <label
              style={{
                display: "block",
                marginBottom: "0.25rem",
                fontWeight: 500,
              }}
            >
              State
            </label>
            <select
              value={selectedState}
              onChange={(e) => setSelectedState(e.target.value)}
              style={{
                width: "100%",
                padding: "0.4rem 0.6rem",
                borderRadius: "4px",
                border: "1px solid #ccc",
                fontSize: "0.9rem",
              }}
            >
              <option value="ALL">All states</option>
              {uniqueStates.map((st) => (
                <option key={st} value={st}>
                  {st}
                </option>
              ))}
            </select>
          </div>

          {/* Chain filter */}
          <div style={{ marginBottom: "1rem" }}>
            <label
              style={{
                display: "block",
                marginBottom: "0.25rem",
                fontWeight: 500,
              }}
            >
              Chain
            </label>
            <select
              value={selectedChain}
              onChange={(e) => setSelectedChain(e.target.value)}
              style={{
                width: "100%",
                padding: "0.4rem 0.6rem",
                borderRadius: "4px",
                border: "1px solid #ccc",
                fontSize: "0.9rem",
              }}
            >
              <option value="ALL">All chains</option>
              {uniqueChains.map((ch) => (
                <option key={ch} value={ch}>
                  {ch}
                </option>
              ))}
            </select>
          </div>

          {/* Clear filters button */}
          <button
            onClick={handleClearFilters}
            style={{
              padding: "0.4rem 0.6rem",
              borderRadius: "4px",
              border: "1px solid #ccc",
              backgroundColor: "white",
              cursor: "pointer",
              fontSize: "0.85rem",
              marginBottom: "1rem",
            }}
          >
            Clear filters
          </button>

          <div
            style={{
              marginTop: "auto",
              padding: "0.75rem",
              borderRadius: "6px",
              border: "1px dashed #ccc",
              fontSize: "0.8rem",
              color: "#777",
            }}
          >
            Pins are color-coded by price for the selected product:
            <ul style={{ margin: "0.25rem 0 0 1.1rem", padding: 0 }}>
              <li>Green = lower price</li>
              <li>Red = higher price</li>
              <li>
                No product selected = all stores, neutral blue pins/clusters.
              </li>
            </ul>
          </div>
        </div>
      )}

      {/* Main map area */}
      <div
        style={{
          flex: 1,
          position: "relative",
          height: "100%",
          overflow: "hidden",
        }}
      >
        <button
          onClick={() => setIsSidebarOpen((prev) => !prev)}
          style={{
            position: "absolute",
            top: "10px",
            right: "10px",
            zIndex: 1000,
            padding: "0.4rem 0.6rem",
            borderRadius: "4px",
            border: "1px solid #ccc",
            backgroundColor: "white",
            cursor: "pointer",
            fontSize: "0.85rem",
          }}
        >
          {isSidebarOpen ? "Hide Filters" : "Show Filters"}
        </button>

        {loadingStores ? (
          <div
            style={{
              display: "flex",
              height: "100%",
              alignItems: "center",
              justifyContent: "center",
            }}
          >
            <p>Loading map…</p>
          </div>
        ) : (
          <PriceMap stores={storesForMap} />
        )}
      </div>
    </div>
  );
}

export default App;
