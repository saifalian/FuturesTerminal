export default function LayerCard({ title, value }: { title: string; value: string }) {
  return (
    <section className="panel">
      <div className="small-title">{title}</div>
      <div>{value}</div>
    </section>
  );
}
