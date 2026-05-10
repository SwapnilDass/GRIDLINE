import { useEffect, useState } from "react";
import "./App.css";

function App() {
  const [drivers, setDrivers] = useState([]);
  const [loading, setLoading] = useState(true);

  const fetchData = async () => {
    const res = await fetch("http://localhost:8000/f1/live");
    const data = await res.json();
    setDrivers(data);
    setLoading(false);
  };

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 5000);
    return () => clearInterval(interval);
  }, []);

  if (loading) return <p>Loading...</p>;

  return (
    <div className="timing-board">
      <h1>F1 Live Timing</h1>
      <table>
        <thead>
          <tr>
            <th>POS</th>
            <th>Driver</th>
            <th>Team</th>
          </tr>
        </thead>
        <tbody>
          {drivers.map((d) => (
            <tr key={d.driver_number}>
              <td>{d.position}</td>
              <td>
                <span
                  style={{
                    borderLeft: `4px solid #${d.team_colour}`,
                    paddingLeft: "8px",
                  }}
                >
                  {d.name_acronym} — {d.full_name}
                </span>
              </td>
              <td>{d.team_name}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default App;
