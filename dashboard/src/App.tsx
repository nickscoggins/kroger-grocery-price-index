import { useEffect, useMemo, useState } from "react";
import { supabase } from "./lib/supabaseClient";
import { PriceMap } from "./components/PriceMap";
import type { StorePoint } from "./components/PriceMap";

// Product type for products table
interface Product {
  upc: string;
  description: string | null;
  brand: string | null;
  categories: string | null;
}

interface StorePriceRow {
  location_id: string;
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
  const [selectedProductUpc, setSelectedProductUpc] = useState<string>("");
  const [selectedState, setSelectedState] = useState<string>("ALL");
  const [selectedChain, setSelectedChain] = useState<string>("ALL");

  // Price data for the selected product
  const [productPrices, setProductPrices] = useState<StorePriceRow[]>([]);
  const [loadingPrices, setLoadingPrices] = useState(false);

  const handleClearFilters = () => {
    setSelectedState("ALL");
    setSelectedChain("ALL");

    if (products.length > 0) {
      setSelectedProductUpc(products[0].upc);
    } else {
      setSelectedProductUpc("");
    }
  };

  // --- Fetch STORES once on mount ---
  useEffect(() => {
    async function fetchStores() {
      setLoadingStores(true);

      // Supabase/PostgREST usually caps at 1000 rows per request,
      // so we fetch in chunks and merge.
      const pageSize = 1000;

      const ranges: [number, number][] = [
        [0, pageSize - 1],           // 0–999
        [pageSize, 2 * pageSize - 1],// 1000–1999
        [2 * pageSize, 3 * pageSize - 1], // 2000–2999 (enough to cover 2,866)
      ];

      try {
        const results = await Promise.all(
          ranges.map(([from, to]) =>
            supabase
              .from("stores")
              .select("location_id, name, chain, city, state, latitude, longitude")
              .range(from, to)
          )
        );

        const allData = results.flatMap((res) => res.data ?? []);

        // Optional: de-duplicate by location_id (in case of overlaps)
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

        // Optionally auto-select first product
        if (productData.length > 0) {
          setSelectedProductUpc(productData[0].upc);
        }
      }

      setLoadingProducts(false);
    }

    fetchProducts();
  }, []);


  // --- Fetch latest prices per store for the selected product ---
  useEffect(() => {
    async function fetchPricesForProduct() {
      if (!selectedProductUpc) {
        setProductPrices([]);
        return;
      }

      setLoadingPrices(true);

      const { data, error } = await supabase
        .from("latest_prices")
        .select("location_id, price_date, regular_price, promo_price")
        .eq("upc", selectedProductUpc)
        .limit(5000); // more than enough for 2,866 stores

      if (error) {
        console.error("Error fetching prices for product:", error);
        setProductPrices([]);
        setLoadingPrices(false);
        return;
      }

      const rows = (data ?? []) as {
        location_id: string;
        price_date: string;
        regular_price: number | null;
        promo_price: number | null;
      }[];

      // latest_prices should already be one row per (location_id, upc),
      // but we keep the same StorePriceRow shape for the rest of the code.
      const latestByLocation = new Map<string, StorePriceRow>();
      for (const r of rows) {
        latestByLocation.set(r.location_id, {
          location_id: r.location_id,
          price_date: r.price_date,
          regular_price: r.regular_price,
          promo_price: r.promo_price,
        });
      }

      setProductPrices(Array.from(latestByLocation.values()));
      setLoadingPrices(false);
    }

    fetchPricesForProduct();
  }, [selectedProductUpc]);



  // --- Derive unique states and chains from stores for filter dropdowns ---
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
      const matchesState =
        selectedState === "ALL" || s.state === selectedState;
      const matchesChain =
        selectedChain === "ALL" || s.chain === selectedChain;
      return matchesState && matchesChain;
    });
  }, [stores, selectedState, selectedChain]);

  // --- Attach prices to filtered stores ---
  const storesWithPrice = useMemo(() => {
    if (productPrices.length === 0) {
      // No prices fetched yet or no data for this product
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


  return (
    <div
      style={{
        display: "flex",
        height: "100vh", // fullscreen app
        width: "100vw", // span full viewport width
        fontFamily: "system-ui, -apple-system, BlinkMacSystemFont, sans-serif",
      }}
    >
      {/* Side panel (conditionally rendered) */}
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
                Stores loaded: {stores.length} (showing {filteredStores.length}{" "}
                after filters)
              </p>
            )}
            {loadingProducts ? (
              <p>Loading products…</p>
            ) : (
              <p style={{ margin: 0 }}>
                Products loaded: {products.length}
              </p>
            )}
          </div>

          {/* Product selector */}
          <div style={{ marginBottom: "1rem" }}>
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
                {products.map((p) => (
                  <option key={p.upc} value={p.upc}>
                    {p.brand ? `${p.brand} – ` : ""}
                    {p.description || "(No description)"} ({p.upc})
                  </option>
                ))}
              </select>
            )}
            <p style={{ margin: "0.25rem 0 0", fontSize: "0.8rem", color: "#777" }}>
              (Price & color-coding will use this selection in the next step.)
            </p>
          </div>

          {/* Location filters */}
          <div style={{ marginBottom: "1rem" }}>
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
            Next up:
            <ul style={{ margin: "0.25rem 0 0 1.1rem", padding: 0 }}>
              <li>Show price data in store popups</li>
              <li>Color-code pins by price for selected product</li>
            </ul>
          </div>
        </div>
      )}

      {/* Main map area */}
      <div style={{ flex: 1, position: "relative" }}>
        {/* Toggle button overlay */}
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
              height: "100vh",
              alignItems: "center",
              justifyContent: "center",
            }}
          >
            <p>Loading map…</p>
          </div>
        ) : (
          <PriceMap stores={storesWithPrice} />
        )}
      </div>
    </div>
  );
}

export default App;
