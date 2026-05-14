// Патерн проєктування: Template Method
// Практичне завдання №1 — Макаров Дмитро, ПЗПІ-23-7
// Підсистема обробки документів різних форматів

// Абстрактний базовий клас з шаблонним методом
public abstract class DocumentProcessor
{
    // Шаблонний метод визначає строгий скелет алгоритму
    public void ProcessDocument()
    {
        OpenDocument();
        ExtractContent();
        if (NeedsFormatting())
        {
            ApplyFormatting();
        }
        SaveDocument();
    }

    // Інваріантні кроки (реалізовані у базовому класі)
    protected void OpenDocument()
    {
        Console.WriteLine("Opening...");
    }

    protected void SaveDocument()
    {
        Console.WriteLine("Saving...");
    }

    // Абстрактний (варіативний) крок
    protected abstract void ExtractContent();

    // Хук — за замовчуванням повертає false
    protected virtual bool NeedsFormatting() => false;

    // Хук для застосування форматування
    protected virtual void ApplyFormatting() { }
}

// Конкретний клас для PDF-документів
public class PDFProcessor : DocumentProcessor
{
    protected override void ExtractContent()
    {
        Console.WriteLine("Extract PDF");
    }
}

// Конкретний клас для Word-документів
public class WordProcessor : DocumentProcessor
{
    protected override void ExtractContent()
    {
        Console.WriteLine("Extract Word");
    }

    protected override bool NeedsFormatting() => true;

    protected override void ApplyFormatting()
    {
        Console.WriteLine("Apply styles");
    }
}

// Клієнтський код
public class Program
{
    public static void Main(string[] args)
    {
        Console.WriteLine("--- PDF ---");
        DocumentProcessor pdf = new PDFProcessor();
        pdf.ProcessDocument();

        Console.WriteLine("\n--- Word ---");
        DocumentProcessor word = new WordProcessor();
        word.ProcessDocument();
    }
}
